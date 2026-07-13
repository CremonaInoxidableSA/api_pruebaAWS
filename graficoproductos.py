import json
from decimal import Decimal
from datetime import datetime
from collections import defaultdict

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


def obtener_datos_buffer(tabla_nombre, region):
    """Obtiene todos los datos de la tabla Buffer."""
    try:
        session = boto3.Session(region_name=region)
        dynamodb_client = session.client("dynamodb", region_name=region)
        
        respuesta = dynamodb_client.scan(TableName=tabla_nombre)
        items = respuesta.get("Items", [])
        
        return [convertir_dynamodb(item) for item in items]
    
    except Exception as e:
        print(f"Error leyendo tabla {tabla_nombre}: {e}")
        return []


def obtener_todas_recetas(tabla_nombre, region):
    """Obtiene todas las recetas de la tabla."""
    try:
        session = boto3.Session(region_name=region)
        dynamodb_client = session.client("dynamodb", region_name=region)
        
        respuesta = dynamodb_client.scan(TableName=tabla_nombre)
        items = respuesta.get("Items", [])
        
        recetas = {}
        for item in items:
            receta_convertida = convertir_dynamodb(item)
            if "nombre_receta" in receta_convertida:
                recetas[receta_convertida["nombre_receta"]] = receta_convertida
        
        return recetas
    
    except Exception as e:
        print(f"Error leyendo tabla de recetas {tabla_nombre}: {e}")
        return {}


def parsear_nivel_json(nivel_str):
    """
    Parsea un string JSON de nivel y retorna los valores.
    Estructura: [cancelaciones, finalizado, seleccionado, tiempo_segundos]
    """
    try:
        if not nivel_str:
            return {"cancelaciones": 0, "finalizado": 0, "seleccionado": 0, "tiempo_segundos": 0}
        
        if isinstance(nivel_str, str):
            datos = json.loads(nivel_str)
        else:
            datos = nivel_str
        
        cancelaciones = datos[0].get("N", 0) if isinstance(datos[0], dict) else datos[0]
        finalizado = datos[1].get("N", 0) if isinstance(datos[1], dict) else datos[1]
        seleccionado = datos[2].get("N", 0) if isinstance(datos[2], dict) else datos[2]
        tiempo_segundos = datos[3].get("N", 0) if isinstance(datos[3], dict) else datos[3]
        
        return {
            "cancelaciones": int(cancelaciones),
            "finalizado": int(finalizado),
            "seleccionado": int(seleccionado),
            "tiempo_segundos": int(tiempo_segundos)
        }
    except:
        return {"cancelaciones": 0, "finalizado": 0, "seleccionado": 0, "tiempo_segundos": 0}


def convertir_fecha_string_a_datetime(fecha_str):
    """Convierte string de fecha en formato DD-MM-YYYY a datetime con 00:00:00."""
    try:
        return datetime.strptime(fecha_str, "%d-%m-%Y")
    except:
        return None


def convertir_fecha_string_a_datetime_fin(fecha_str):
    """Convierte string de fecha en formato DD-MM-YYYY a datetime con 23:59:59."""
    try:
        fecha_dt = datetime.strptime(fecha_str, "%d-%m-%Y")
        return fecha_dt.replace(hour=23, minute=59, second=59)
    except:
        return None


def convertir_fecha_string_completa_a_datetime(fecha_str):
    """Convierte string de fecha en formato DD-MM-YYYY HH:MM:SS a datetime."""
    try:
        return datetime.strptime(fecha_str, "%d-%m-%Y %H:%M:%S")
    except:
        return None


def calcular_tiempo_diferencia_segundos(fecha_inicio, fecha_fin):
    """Calcula la diferencia entre dos fechas y retorna en segundos."""
    if not fecha_inicio or not fecha_fin:
        return 0
    
    try:
        if isinstance(fecha_inicio, str):
            fecha_inicio = convertir_fecha_string_completa_a_datetime(fecha_inicio)
            if not fecha_inicio:
                fecha_inicio = convertir_fecha_string_a_datetime(fecha_inicio)
        if isinstance(fecha_fin, str):
            fecha_fin = convertir_fecha_string_completa_a_datetime(fecha_fin)
            if not fecha_fin:
                fecha_fin = convertir_fecha_string_a_datetime(fecha_fin)
        
        if not fecha_inicio or not fecha_fin:
            return 0
        
        diferencia = fecha_fin - fecha_inicio
        segundos_totales = int(diferencia.total_seconds())
        
        return max(0, segundos_totales)
    except:
        return 0


def convertir_segundos_a_hhmmss(segundos):
    """Convierte segundos a formato HH:MM:SS."""
    horas = segundos // 3600
    minutos = (segundos % 3600) // 60
    secs = segundos % 60
    return f"{horas:02d}:{minutos:02d}:{secs:02d}"


def obtener_grafico_productos(fecha_inicio, fecha_fin, tabla_buffer, tabla_receta, region):
    """
    Genera datos para gráfico de barras agrupando por receta y fecha.
    
    Args:
        fecha_inicio: Fecha inicio en formato DD-MM-YYYY
        fecha_fin: Fecha fin en formato DD-MM-YYYY
        tabla_buffer: Nombre de la tabla Buffer en DynamoDB
        tabla_receta: Nombre de la tabla Receta en DynamoDB
        region: Región de AWS
    
    Returns:
        Lista de diccionarios con datos para gráfico
    """
    try:
        # Convertir fechas
        fecha_inicio_dt = convertir_fecha_string_a_datetime(fecha_inicio)
        fecha_fin_dt = convertir_fecha_string_a_datetime_fin(fecha_fin)
        
        if not fecha_inicio_dt or not fecha_fin_dt:
            return []
        
        if fecha_inicio_dt > fecha_fin_dt:
            return []
        
        # Obtener datos
        buffer_data = obtener_datos_buffer(tabla_buffer, region)
        recetas = obtener_todas_recetas(tabla_receta, region)
        
        if not buffer_data or not recetas:
            return []
        
        # Agrupar datos por receta y fecha
        # Clave: (nombre_receta, fecha_YYYY-MM-DD)
        grupos = defaultdict(lambda: {"kilos": 0.0, "segundos": 0})
        
        for buffer_item in buffer_data:
            fecha_item_inicio = buffer_item.get("fecha_inicio", "")
            fecha_item_fin = buffer_item.get("fecha_fin", "")
            
            # Convertir fechas del item
            fecha_item_inicio_dt = convertir_fecha_string_completa_a_datetime(fecha_item_inicio)
            fecha_item_fin_dt = convertir_fecha_string_completa_a_datetime(fecha_item_fin)
            
            if not fecha_item_inicio_dt or not fecha_item_fin_dt:
                continue
            
            # Verificar si el registro está dentro del rango de fechas
            if fecha_inicio_dt <= fecha_item_inicio_dt <= fecha_fin_dt:
                receta_nombre = buffer_item.get("recetaBuffer1", "")
                receta = recetas.get(receta_nombre, {})
                peso_producto = receta.get("peso_producto", 0) or 0
                productos_nivel = receta.get("productos_nivel", 0) or 0
                
                try:
                    peso_producto = float(peso_producto) if peso_producto else 0
                    productos_nivel = int(productos_nivel) if productos_nivel else 0
                except:
                    peso_producto = 0
                    productos_nivel = 0
                
                # Contar niveles finalizados
                niveles_finalizados = 0
                for num_nivel in range(1, 14):
                    nivel_key = f"nivel{num_nivel}"
                    nivel_data = buffer_item.get(nivel_key)
                    
                    if nivel_data:
                        nivel_info = parsear_nivel_json(nivel_data)
                        if nivel_info["finalizado"] == 1:
                            niveles_finalizados += 1
                
                # Calcular kilos del registro
                kilos_del_registro = niveles_finalizados * peso_producto * productos_nivel
                
                # Calcular tiempo neto
                tiempo_neto_segundos = calcular_tiempo_diferencia_segundos(
                    fecha_item_inicio_dt, 
                    fecha_item_fin_dt
                )
                
                # Extraer fecha en formato YYYY-MM-DD
                fecha_formateada = fecha_item_inicio_dt.strftime("%Y-%m-%d")
                
                # Clave del grupo
                clave_grupo = (receta_nombre, fecha_formateada)
                
                # Agregar al grupo
                grupos[clave_grupo]["kilos"] += kilos_del_registro
                grupos[clave_grupo]["segundos"] += tiempo_neto_segundos
        
        # Convertir grupos a lista con IDs
        resultado = []
        id_counter = 1
        
        for (receta_nombre, fecha), datos in sorted(grupos.items()):
            resultado.append({
                "id": id_counter,
                "nombre": receta_nombre,
                "fecha": fecha,
                "tiempo": convertir_segundos_a_hhmmss(datos["segundos"]),
                "kilogramos": round(datos["kilos"], 2)
            })
            id_counter += 1
        
        return resultado
        
    except Exception as e:
        print(f"Error generando gráfico de productos: {e}")
        return []
