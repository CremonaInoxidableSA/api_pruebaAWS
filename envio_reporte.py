import os
import asyncio
import logging
from typing import List, Optional
from email.message import EmailMessage
from dotenv import load_dotenv
import aiosmtplib

from reporte_racks import export_racks_to_excel
import tempfile

load_dotenv()

logger = logging.getLogger("uvicorn")

EMAIL_FROM: Optional[str] = os.getenv("EMAIL_FROM")
SMTP_USER: Optional[str] = os.getenv("SMTP_USER")
SMTP_PASS: Optional[str] = os.getenv("SMTP_PASS")
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
LISTA_CORREOS_FILE = "lista_correos.txt"

ASUNTO_REPORTE = "Reporte | Demo AWS"
CUERPO_REPORTE = """
<html>
    <body>
        <p>Estimados, les compartimos un documento de muestra, el cual permitirá verificar la funcionalidad de envio de correos a través de servicios de AWS. Saludos.</p>
    </body>
</html>
"""


def obtener_emails_destinatarios() -> List[str]:
    """Lee los correos de la lista_correos.txt"""
    emails: List[str] = []
    try:
        if os.path.exists(LISTA_CORREOS_FILE):
            with open(LISTA_CORREOS_FILE, "r", encoding="utf-8") as f:
                for linea in f:
                    email = linea.strip()
                    if email and not email.startswith("#") and "@" in email:
                        emails.append(email)
            logger.info(f"✓ Se cargaron {len(emails)} emails desde {LISTA_CORREOS_FILE}")
    except Exception as e:
        logger.error(f"Error leyendo lista de correos: {e}")
    
    return emails


def validar_configuracion_smtp() -> bool:
    """Valida que las credenciales SMTP estén configuradas"""
    if not all([EMAIL_FROM, SMTP_USER, SMTP_PASS]):
        logger.error("Error: Credenciales de email no configuradas (EMAIL_FROM, SMTP_USER o SMTP_PASS faltantes).")
        return False
    return True


def validar_email(email: str) -> bool:
    """Valida que el formato del email sea correcto"""
    return bool(email and "@" in email)


async def enviar_email_con_adjunto(
    destinatario: str, 
    asunto: str, 
    cuerpo_html: str,
    archivo_adjunto: str,
    nombre_archivo: str,
    max_retries: int = 2
) -> bool:
    """
    Envía un email con un archivo adjunto
    
    Args:
        destinatario: Email del destinatario
        asunto: Asunto del correo
        cuerpo_html: Cuerpo en HTML
        archivo_adjunto: Ruta del archivo a adjuntar
        nombre_archivo: Nombre con el que aparecerá el archivo adjunto
        max_retries: Número máximo de reintentos
    
    Returns:
        True si se envió exitosamente, False si falló
    """
    if not validar_configuracion_smtp():
        return False
    
    if not validar_email(destinatario):
        logger.error(f"Email destino inválido: {destinatario}")
        return False
    
    if not os.path.exists(archivo_adjunto):
        logger.error(f"Archivo adjunto no existe: {archivo_adjunto}")
        return False
    
    logger.info(f"Preparando envío de email a {destinatario} con adjunto {nombre_archivo}")
    
    message = EmailMessage()
    message["Subject"] = asunto
    message["From"] = EMAIL_FROM
    message["To"] = destinatario
    
    message.set_content("Email en formato HTML - utilizar cliente con soporte para HTML")
    message.add_alternative(cuerpo_html, subtype="html")
    
    # Adjuntar archivo
    try:
        with open(archivo_adjunto, "rb") as attachment:
            data = attachment.read()
            message.add_attachment(
                data,
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=nombre_archivo
            )
    except Exception as e:
        logger.error(f"Error adjuntando archivo: {e}")
        return False
    
    assert EMAIL_FROM is not None
    assert SMTP_USER is not None
    assert SMTP_PASS is not None
    
    for intento in range(max_retries):
        try:
            smtp = aiosmtplib.SMTP(hostname=SMTP_HOST, port=SMTP_PORT, start_tls=True)
            await smtp.connect()
            try:
                await smtp.login(SMTP_USER, SMTP_PASS)
                await smtp.sendmail(EMAIL_FROM, [destinatario], message.as_string())
                await smtp.quit()
                
                logger.info(f"✓ Email enviado exitosamente a {destinatario} (intento {intento + 1}/{max_retries})")
                return True
            except Exception as send_error:
                try:
                    await smtp.quit()
                except:
                    pass
                raise send_error
            
        except Exception as e:
            logger.warning(f"Intento {intento + 1}/{max_retries} fallido para {destinatario}: {e}")
            
            if intento < max_retries - 1:
                await asyncio.sleep(2 ** intento)
            else:
                logger.error(f"Error final al enviar email a {destinatario}: {e}")
                return False
    
    return False


async def enviar_reporte_racks(tabla_buffer: str, tabla_receta: str, region: str) -> dict:
    """
    Genera el reporte de racks y lo envía a todos los correos en lista_correos.txt
    
    Args:
        tabla_buffer: Nombre de la tabla Buffer en DynamoDB
        tabla_receta: Nombre de la tabla Receta en DynamoDB
        region: Región de AWS
    
    Returns:
        Diccionario con información del envío
    """
    resultado = {
        "exitoso": False,
        "total_correos": 0,
        "correos_enviados": 0,
        "correos_fallidos": [],
        "mensaje": ""
    }
    
    # Obtener lista de correos
    emails_destinatarios = obtener_emails_destinatarios()
    
    if not emails_destinatarios:
        resultado["mensaje"] = "No se encontraron correos en lista_correos.txt"
        logger.error(resultado["mensaje"])
        return resultado
    
    resultado["total_correos"] = len(emails_destinatarios)
    
    # Generar reporte
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            temp_path = temp_file.name
        
        logger.info("Generando reporte...")
        success = export_racks_to_excel(temp_path, tabla_buffer, tabla_receta, region)
        
        if not success:
            resultado["mensaje"] = "No se pudo generar el reporte"
            logger.error(resultado["mensaje"])
            return resultado
        
        # Enviar a cada correo
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"reporte_racks_{timestamp}.xlsx"
        
        logger.info(f"Enviando reporte a {len(emails_destinatarios)} correo(s)...")
        
        for email in emails_destinatarios:
            try:
                enviado = await enviar_email_con_adjunto(
                    destinatario=email,
                    asunto=ASUNTO_REPORTE,
                    cuerpo_html=CUERPO_REPORTE,
                    archivo_adjunto=temp_path,
                    nombre_archivo=nombre_archivo
                )
                
                if enviado:
                    resultado["correos_enviados"] += 1
                else:
                    resultado["correos_fallidos"].append(email)
                    
            except Exception as e:
                logger.error(f"Error enviando a {email}: {e}")
                resultado["correos_fallidos"].append(email)
        
        # Limpiar archivo temporal
        try:
            os.remove(temp_path)
        except:
            pass
        
        resultado["exitoso"] = resultado["correos_enviados"] > 0
        resultado["mensaje"] = f"Enviados {resultado['correos_enviados']}/{resultado['total_correos']} correos"
        
        logger.info(resultado["mensaje"])
        
        return resultado
        
    except Exception as e:
        resultado["mensaje"] = f"Error: {str(e)}"
        logger.error(resultado["mensaje"])
        return resultado
