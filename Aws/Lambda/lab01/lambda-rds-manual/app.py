import os
import boto3
from chalice import Chalice, BadRequestError

app = Chalice(app_name='lambda-rds-manual')
app.debug = True

# Configurações (melhor usar Parameter Store/Secrets Manager em produção)
RDS_INSTANCE_ID = os.getenv('RDS_INSTANCE_ID', 'database-1')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

@app.route('/start', methods=['POST'], api_key_required=True)
def start_rds():
    """Inicia a instância RDS manualmente"""
    client = boto3.client('rds', region_name=AWS_REGION)
    
    try:
        response = client.start_db_instance(
            DBInstanceIdentifier=RDS_INSTANCE_ID
        )
        return {
            'status': 'success',
            'action': 'start',
            'response': response
        }
    except client.exceptions.InvalidDBInstanceStateFault:
        return {'status': 'error', 'message': 'Instância já está em execução ou estado inválido'}
    except Exception as e:
        raise BadRequestError(f"Erro ao iniciar RDS: {str(e)}")

@app.route('/stop', methods=['POST'], api_key_required=True)
def stop_rds():
    """Para a instância RDS manualmente"""
    client = boto3.client('rds', region_name=AWS_REGION)
    
    try:
        response = client.stop_db_instance(
            DBInstanceIdentifier=RDS_INSTANCE_ID
        )
        return {
            'status': 'success',
            'action': 'stop',
            'response': response
        }
    except client.exceptions.InvalidDBInstanceStateFault:
        return {'status': 'error', 'message': 'Instância já está parada ou estado inválido'}
    except Exception as e:
        raise BadRequestError(f"Erro ao parar RDS: {str(e)}")

# Rota para ver status (opcional)
@app.route('/status', methods=['GET'])
def get_status():
    client = boto3.client('rds', region_name=AWS_REGION)
    response = client.describe_db_instances(
        DBInstanceIdentifier=RDS_INSTANCE_ID
    )
    status = response['DBInstances'][0]['DBInstanceStatus']
    return {'status': status}
