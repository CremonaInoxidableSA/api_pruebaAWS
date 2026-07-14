import asyncio
from decimal import Decimal

import boto3


def convertir_dynamodb(item):
    """Convierte formato de tipos de DynamoDB al formato normal JSON."""
    if item is None:
        return None
    
    nuevo = {}
    for clave, valor in item.items():
        if isinstance(valor, dict):
            if "N" in valor:
                val = Decimal(valor["N"])
                nuevo[clave] = int(val) if val % 1 == 0 else float(val)
            elif "S" in valor:
                nuevo[clave] = valor["S"]
            elif "L" in valor:
                nuevo[clave] = [convertir_dynamodb({"item": v})["item"] for v in valor["L"]]
            elif "M" in valor:
                nuevo[clave] = convertir_dynamodb(valor["M"])
            elif "BOOL" in valor:
                nuevo[clave] = valor["BOOL"]
            elif "NULL" in valor:
                nuevo[clave] = None
            else:
                nuevo[clave] = valor
        else:
            nuevo[clave] = valor
    return nuevo


def convertir_dynamodb_sin_timestamp(item):
    """Convierte formato de DynamoDB a JSON y elimina el campo timestamp."""
    convertido = convertir_dynamodb(item)
    if convertido and "timestamp" in convertido:
        del convertido["timestamp"]
    return convertido


async def obtener_ultimo_registro(tabla_nombre, region):
    """Obtiene el registro más reciente (por timestamp) de una tabla DynamoDB."""
    try:
        session = boto3.Session(region_name=region)
        dynamodb_client = session.client("dynamodb", region_name=region)
        
        # Escanear múltiples items para encontrar el más reciente por timestamp
        respuesta = dynamodb_client.scan(TableName=tabla_nombre, Limit=100)
        items = respuesta.get("Items", [])
        
        if not items:
            return None
        
        # Encontrar el item con el timestamp más reciente
        ultimo_item = items[0]
        ultima_fecha = ultimo_item.get("timestamp", {}).get("S", "")
        
        for item in items[1:]:
            fecha = item.get("timestamp", {}).get("S", "")
            if fecha > ultima_fecha:
                ultimo_item = item
                ultima_fecha = fecha
        
        return ultimo_item
    except Exception as e:
        print(f"Error obteniendo último registro de {tabla_nombre}: {e}")
        return None


async def obtener_ultimos_botones(tabla_nombre, region):
    """Obtiene el último estado de cada botón (agrupado por numeroBoton)."""
    try:
        session = boto3.Session(region_name=region)
        dynamodb_client = session.client("dynamodb", region_name=region)
        
        ultimos_botones = {}
        respuesta = dynamodb_client.scan(TableName=tabla_nombre)
        
        for item in respuesta.get("Items", []):
            numero_boton_field = item.get("numeroBoton")
            if numero_boton_field and isinstance(numero_boton_field, dict):
                # Extraer el valor del formato DynamoDB {"N": "2"} o {"S": "2"}
                numero_boton = numero_boton_field.get("N") or numero_boton_field.get("S")
                if numero_boton:
                    ultimos_botones[numero_boton] = item
        
        # Continuar si hay más items
        while "LastEvaluatedKey" in respuesta:
            respuesta = dynamodb_client.scan(TableName=tabla_nombre, ExclusiveStartKey=respuesta["LastEvaluatedKey"])
            for item in respuesta.get("Items", []):
                numero_boton_field = item.get("numeroBoton")
                if numero_boton_field and isinstance(numero_boton_field, dict):
                    numero_boton = numero_boton_field.get("N") or numero_boton_field.get("S")
                    if numero_boton:
                        ultimos_botones[numero_boton] = item
        
        return list(ultimos_botones.values()) if ultimos_botones else []
    except Exception as e:
        print(f"Error obteniendo últimos botones de {tabla_nombre}: {e}")
        return []


async def monitorear_tabla_tiemporeal(tabla_nombre, region, clientes):
    """Monitorea cambios en una tabla DynamoDB y notifica a clientes.
    Envía el último registro a nuevos clientes que se conecten."""
    try:
        print(f"Intentando conectar a tabla '{tabla_nombre}' en región '{region}'...")
        session = boto3.Session(region_name=region)
        dynamodb_client = session.client("dynamodb", region_name=region)
        
        respuesta = dynamodb_client.describe_table(TableName=tabla_nombre)
        stream_arn = respuesta["Table"].get("LatestStreamArn")
        
        if not stream_arn:
            print(f"No se encontró stream en la tabla {tabla_nombre}")
            return
        
        print(f"Stream encontrado: {stream_arn}")
        
        streams = session.client("dynamodbstreams", region_name=region)
        clientes_notificados = set()
        shard_iteradores = {}
        
        while True:
            try:
                # Obtener shards del stream
                respuesta_stream = streams.describe_stream(StreamArn=stream_arn)
                shards = respuesta_stream["StreamDescription"]["Shards"]
                
                # Inicializar iteradores si no existen
                for shard in shards:
                    shard_id = shard["ShardId"]
                    if shard_id not in shard_iteradores:
                        respuesta_iter = streams.get_shard_iterator(
                            StreamArn=stream_arn,
                            ShardId=shard_id,
                            ShardIteratorType="LATEST",
                        )
                        shard_iteradores[shard_id] = respuesta_iter["ShardIterator"]
                
                # Verificar y enviar a clientes nuevos
                clientes_nuevos = clientes - clientes_notificados
                if clientes_nuevos:
                    print(f"Nuevos clientes detectados en {tabla_nombre}: {len(clientes_nuevos)}")
                    ultimo_registro = await obtener_ultimo_registro(tabla_nombre, region)
                    if ultimo_registro:
                        ultimo_registro_convertido = convertir_dynamodb_sin_timestamp(ultimo_registro)
                        for websocket in clientes_nuevos.copy():
                            try:
                                print(f"Enviando último registro a cliente en {tabla_nombre}")
                                await websocket.send_json(ultimo_registro_convertido)
                                clientes_notificados.add(websocket)
                            except Exception as e:
                                print(f"Error enviando a cliente: {e}")
                                clientes.discard(websocket)
                
                # Leer registros de todos los shards
                for shard_id, iterador in list(shard_iteradores.items()):
                    if iterador is None:
                        continue
                    
                    try:
                        respuesta = streams.get_records(ShardIterator=iterador, Limit=100)
                        
                        for evento in respuesta.get("Records", []):
                            if evento["eventName"] in ["INSERT", "MODIFY"]:
                                datos = evento["dynamodb"].get("NewImage", {})
                                datos_convertidos = convertir_dynamodb_sin_timestamp(datos)
                                
                                for websocket in clientes.copy():
                                    try:
                                        await websocket.send_json(datos_convertidos)
                                    except:
                                        clientes.discard(websocket)
                                        clientes_notificados.discard(websocket)
                        
                        shard_iteradores[shard_id] = respuesta.get("NextShardIterator")
                    
                    except Exception as e:
                        print(f"Error leyendo shard {shard_id}: {e}")
                        shard_iteradores[shard_id] = None
                
                await asyncio.sleep(0.5)
            
            except Exception as e:
                print(f"Error monitoreando: {e}")
                await asyncio.sleep(2)
    
    except Exception as e:
        print(f"Error conectando a DynamoDB: {e}")
        print(f"Verifica que:")
        print(f"   - Tabla: {tabla_nombre}")
        print(f"   - Región: {region}")
        print(f"   - Credenciales AWS configuradas")

async def monitorear_tabla_botones(tabla_nombre, region, clientes):
    """Monitorea cambios en la tabla de botones y notifica a clientes.
    Mantiene el último estado de cada botón y envía todos los estados actualizados.
    Envía todos los estados a nuevos clientes que se conecten."""
    try:
        print(f"Intentando conectar a tabla '{tabla_nombre}' en región '{region}'...")
        session = boto3.Session(region_name=region)
        dynamodb_client = session.client("dynamodb", region_name=region)
        
        respuesta = dynamodb_client.describe_table(TableName=tabla_nombre)
        stream_arn = respuesta["Table"].get("LatestStreamArn")
        
        if not stream_arn:
            print(f"No se encontró stream en la tabla {tabla_nombre}")
            return
        
        print(f"Stream encontrado: {stream_arn}")
        
        streams = session.client("dynamodbstreams", region_name=region)
        clientes_notificados = set()
        ultimos_botones = {}
        shard_iteradores = {}
        
        # Obtener los botones iniciales
        botones_iniciales = await obtener_ultimos_botones(tabla_nombre, region)
        for boton in botones_iniciales:
            numero_boton_field = boton.get("numeroBoton")
            if numero_boton_field and isinstance(numero_boton_field, dict):
                numero_boton = numero_boton_field.get("N") or numero_boton_field.get("S")
                if numero_boton:
                    ultimos_botones[numero_boton] = boton
        
        while True:
            try:
                # Obtener shards del stream
                respuesta_stream = streams.describe_stream(StreamArn=stream_arn)
                shards = respuesta_stream["StreamDescription"]["Shards"]
                
                # Inicializar iteradores si no existen
                for shard in shards:
                    shard_id = shard["ShardId"]
                    if shard_id not in shard_iteradores:
                        respuesta_iter = streams.get_shard_iterator(
                            StreamArn=stream_arn,
                            ShardId=shard_id,
                            ShardIteratorType="LATEST",
                        )
                        shard_iteradores[shard_id] = respuesta_iter["ShardIterator"]
                
                # Verificar y enviar a clientes nuevos
                clientes_nuevos = clientes - clientes_notificados
                if clientes_nuevos:
                    print(f"Nuevos clientes detectados en {tabla_nombre}: {len(clientes_nuevos)}")
                    botones_convertidos = [convertir_dynamodb_sin_timestamp(b) for b in ultimos_botones.values()]
                    for websocket in clientes_nuevos.copy():
                        try:
                            print(f"Enviando {len(botones_convertidos)} botones a cliente en {tabla_nombre}")
                            await websocket.send_json(botones_convertidos)
                            clientes_notificados.add(websocket)
                        except Exception as e:
                            print(f"Error enviando a cliente: {e}")
                            clientes.discard(websocket)
                
                # Leer registros de todos los shards
                for shard_id, iterador in list(shard_iteradores.items()):
                    if iterador is None:
                        continue
                    
                    try:
                        respuesta = streams.get_records(ShardIterator=iterador, Limit=100)
                        
                        for evento in respuesta.get("Records", []):
                            if evento["eventName"] in ["INSERT", "MODIFY"]:
                                datos = evento["dynamodb"].get("NewImage", {})
                                numero_boton_field = datos.get("numeroBoton")
                                
                                # Extraer el número de botón del formato DynamoDB
                                numero_boton = None
                                if numero_boton_field and isinstance(numero_boton_field, dict):
                                    numero_boton = numero_boton_field.get("N") or numero_boton_field.get("S")
                                
                                # Actualizar el último estado de este botón
                                if numero_boton:
                                    ultimos_botones[numero_boton] = datos
                                
                                # Enviar TODOS los estados actualizados de botones a todos los clientes (en un solo mensaje)
                                botones_convertidos = [convertir_dynamodb_sin_timestamp(b) for b in ultimos_botones.values()]
                                
                                for websocket in clientes.copy():
                                    try:
                                        await websocket.send_json(botones_convertidos)
                                    except:
                                        clientes.discard(websocket)
                                        clientes_notificados.discard(websocket)
                        
                        shard_iteradores[shard_id] = respuesta.get("NextShardIterator")
                    
                    except Exception as e:
                        print(f"Error leyendo shard {shard_id}: {e}")
                        shard_iteradores[shard_id] = None
                
                await asyncio.sleep(0.5)
            
            except Exception as e:
                print(f"Error monitoreando: {e}")
                await asyncio.sleep(2)
    
    except Exception as e:
        print(f"Error conectando a DynamoDB: {e}")
        print(f"Verifica que:")
        print(f"   - Tabla: {tabla_nombre}")
        print(f"   - Región: {region}")
        print(f"   - Credenciales AWS configuradas")