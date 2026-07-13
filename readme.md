python -m venv venv
    #Si sale error de permisos en la ejecución de scripts:
    Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
venv\Scripts\activate
pip install --upgrade -r requirements.txt
python main.py


# SE DEBE CONFIGURAR LOS PARAMETROS DE AWS
aws configure
Y activar "Detalles del flujo de DynamoDB" en la tabla de DynamoDB que se quiera consumir en tiempo real.