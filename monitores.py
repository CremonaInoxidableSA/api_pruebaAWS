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


async def monitorear_tabla_tiemporeal(tabla_nombre, region, clientes):
    """Monitorea cambios en una tabla DynamoDB y notifica a clientes."""
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
        
        while True:
            try:
                respuesta_stream = streams.describe_stream(StreamArn=stream_arn)
                shards = respuesta_stream["StreamDescription"]["Shards"]
                
                shard_iteradores = []
                for shard in shards:
                    shard_id = shard["ShardId"]
                    respuesta_iter = streams.get_shard_iterator(
                        StreamArn=stream_arn,
                        ShardId=shard_id,
                        ShardIteratorType="LATEST",
                    )
                    shard_iteradores.append(respuesta_iter["ShardIterator"])
                
                while True:
                    for i, iterador in enumerate(shard_iteradores):
                        if iterador is None:
                            continue
                        
                        try:
                            respuesta = streams.get_records(ShardIterator=iterador, Limit=100)
                            
                            for evento in respuesta.get("Records", []):
                                if evento["eventName"] in ["INSERT", "MODIFY"]:
                                    datos = evento["dynamodb"].get("NewImage", {})
                                    datos_convertidos = convertir_dynamodb(datos)
                                    
                                    for websocket in clientes.copy():
                                        try:
                                            await websocket.send_json(datos_convertidos)
                                        except:
                                            clientes.discard(websocket)
                            
                            shard_iteradores[i] = respuesta.get("NextShardIterator")
                        
                        except Exception as e:
                            print(f"Error leyendo shard: {e}")
                            shard_iteradores[i] = None
                    
                    await asyncio.sleep(1)
            
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
    """Monitorea cambios en la tabla de botones y notifica a clientes."""
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
        
        while True:
            try:
                respuesta_stream = streams.describe_stream(StreamArn=stream_arn)
                shards = respuesta_stream["StreamDescription"]["Shards"]
                
                shard_iteradores = []
                for shard in shards:
                    shard_id = shard["ShardId"]
                    respuesta_iter = streams.get_shard_iterator(
                        StreamArn=stream_arn,
                        ShardId=shard_id,
                        ShardIteratorType="LATEST",
                    )
                    shard_iteradores.append(respuesta_iter["ShardIterator"])
                
                while True:
                    for i, iterador in enumerate(shard_iteradores):
                        if iterador is None:
                            continue
                        
                        try:
                            respuesta = streams.get_records(ShardIterator=iterador, Limit=100)
                            
                            for evento in respuesta.get("Records", []):
                                if evento["eventName"] in ["INSERT", "MODIFY"]:
                                    datos = evento["dynamodb"].get("NewImage", {})
                                    datos_convertidos = convertir_dynamodb(datos)
                                    
                                    for websocket in clientes.copy():
                                        try:
                                            await websocket.send_json(datos_convertidos)
                                        except:
                                            clientes.discard(websocket)
                            
                            shard_iteradores[i] = respuesta.get("NextShardIterator")
                        
                        except Exception as e:
                            print(f"Error leyendo shard: {e}")
                            shard_iteradores[i] = None
                    
                    await asyncio.sleep(1)
            
            except Exception as e:
                print(f"Error monitoreando: {e}")
                await asyncio.sleep(2)
    
    except Exception as e:
        print(f"Error conectando a DynamoDB: {e}")
        print(f"Verifica que:")
        print(f"   - Tabla: {tabla_nombre}")
        print(f"   - Región: {region}")
        print(f"   - Credenciales AWS configuradas")