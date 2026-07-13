import asyncio
import os
import tempfile
from datetime import datetime

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from monitores import monitorear_tabla_tiemporeal, monitorear_tabla_botones
from buffer import obtener_datos_buffer
from reporte_racks import export_racks_to_excel
from reporte_fecha import export_racks_to_excel_por_fecha
from envio_reporte import enviar_reporte_racks
from productividad import obtener_productividad
from graficoproductos import obtener_grafico_productos

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TABLA_NOMBRE = os.getenv("TABLA_NOMBRE", "TiempoReal")
TABLA_NOMBRE2 = os.getenv("TABLA_NOMBRE2", "TiempoReal-Botones")
TABLA_BUFFER = os.getenv("TABLA_BUFFER", "Buffer")
TABLA_RECETA = os.getenv("TABLA_RECETA", "Receta")
REGION = os.getenv("AWS_REGION", "sa-east-1")

clientes_conectados = set()
clientes_conectados2 = set()

@app.on_event("startup")
async def startup():
    asyncio.create_task(monitorear_tabla_tiemporeal(TABLA_NOMBRE, REGION, clientes_conectados))
    asyncio.create_task(monitorear_tabla_botones(TABLA_NOMBRE2, REGION, clientes_conectados2))


@app.websocket("/tiemporeal")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clientes_conectados.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clientes_conectados.discard(websocket)

@app.websocket("/botones")
async def websocket_endpoint2(websocket: WebSocket):
    await websocket.accept()
    clientes_conectados2.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clientes_conectados2.discard(websocket)


@app.get("/buffer")
async def obtener_buffer():
    return obtener_datos_buffer(TABLA_BUFFER, REGION)


@app.get("/reporte/racks")
async def descargar_reporte_racks():
    """Genera y descarga el reporte de racks en formato Excel."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            temp_path = temp_file.name
        
        success = export_racks_to_excel(temp_path, TABLA_BUFFER, TABLA_RECETA, REGION)
        
        if not success:
            return {"error": "No se pudo generar el reporte"}
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reporte_racks_{timestamp}.xlsx"
        
        return FileResponse(
            path=temp_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    except Exception as e:
        return {"error": f"Error al generar el reporte: {str(e)}"}


@app.get("/reporte/racks/enviar")
async def enviar_reporte_por_correo():
    """Genera el reporte de racks y lo envía a los correos en lista_correos.txt."""
    try:
        resultado = await enviar_reporte_racks(TABLA_BUFFER, TABLA_RECETA, REGION)
        return resultado
    
    except Exception as e:
        return {
            "exitoso": False,
            "mensaje": f"Error al enviar reporte: {str(e)}",
            "total_correos": 0,
            "correos_enviados": 0,
            "correos_fallidos": []
        }


@app.get("/reporte/racks/fecha")
async def descargar_reporte_racks_por_fecha(
    fecha_inicio: str = Query(..., description="Fecha inicio en formato DD-MM-YYYY"),
    fecha_fin: str = Query(..., description="Fecha fin en formato DD-MM-YYYY")
):
    """Genera y descarga el reporte de racks filtrados por fecha en formato Excel."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            temp_path = temp_file.name
        
        success = export_racks_to_excel_por_fecha(
            temp_path, TABLA_BUFFER, TABLA_RECETA, REGION, fecha_inicio, fecha_fin
        )
        
        if not success:
            return {"error": "No se pudo generar el reporte"}
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reporte_racks_{timestamp}.xlsx"
        
        return FileResponse(
            path=temp_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    except Exception as e:
        return {"error": f"Error al generar el reporte: {str(e)}"}


@app.get("/productividad")
async def consultar_productividad(
    fecha_inicio: str = Query(..., description="Fecha inicio en formato DD-MM-YYYY"),
    fecha_fin: str = Query(..., description="Fecha fin en formato DD-MM-YYYY")
):
    """
    Retorna métricas de productividad en un rango de fechas.
    
    Parámetros:
    - fecha_inicio: Fecha de inicio (formato: DD-MM-YYYY, fija 00:00:00)
    - fecha_fin: Fecha de fin (formato: DD-MM-YYYY, fija 23:59:59)
    
    Retorna:
    - racks: Recuento de registros en el rango de fechas
    - productos_realizados: Suma de (niveles finalizados * peso_producto * productos_nivel)
    - promedio_uso: Suma de tiempos netos en formato HH:MM
    - porcentaje_producto_realizado: Array con porcentaje y cantidad por receta
    """
    try:
        resultado = obtener_productividad(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            tabla_buffer=TABLA_BUFFER,
            tabla_receta=TABLA_RECETA,
            region=REGION
        )
        return resultado
    
    except Exception as e:
        return {
            "racks": 0,
            "productos_realizados": 0.0,
            "promedio_uso": "00:00",
            "porcentaje_producto_realizado": []
        }


@app.get("/grafico/productos")
async def obtener_datos_grafico_productos(
    fecha_inicio: str = Query(..., description="Fecha inicio en formato DD-MM-YYYY"),
    fecha_fin: str = Query(..., description="Fecha fin en formato DD-MM-YYYY")
):
    """
    Retorna datos para gráfico de barras agrupando por receta y fecha.
    
    Parámetros:
    - fecha_inicio: Fecha de inicio (formato: DD-MM-YYYY)
    - fecha_fin: Fecha de fin (formato: DD-MM-YYYY)
    
    Retorna:
    Array con estructura:
    - id: Identificador secuencial
    - nombre: Nombre de la receta
    - fecha: Fecha en formato YYYY-MM-DD
    - tiempo: Suma de tiempos en formato HH:MM:SS
    - kilogramos: Suma de kilogramos producidos
    """
    try:
        resultado = obtener_grafico_productos(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            tabla_buffer=TABLA_BUFFER,
            tabla_receta=TABLA_RECETA,
            region=REGION
        )
        return resultado
    
    except Exception as e:
        return []


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8500)