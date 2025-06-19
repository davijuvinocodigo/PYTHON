import boto3
from chalice import Chalice, Cron

app = Chalice(app_name='lambda-rds-start-stop')
app.debug = True  # Remova em produção

# Configurações (melhor usar variáveis de ambiente)
RDS_INSTANCE_ID = 'database-1'  # Substitua pelo seu ID
AWS_REGION = 'us-east-1'  # Ajuste para sua região

# Para produção, considere usar AWS Parameter Store ou Secrets Manager
# para armazenar essas configurações

@app.schedule(Cron(0, 21, '*', '*', '?', '*'))  # 21:00 UTC = 18:00 Brasília (UTC-3)
def start_rds(event):
    """Inicia a instância RDS diariamente às 18:00 (horário de Brasília)."""
    rds = boto3.client('rds', region_name=AWS_REGION)
    try:
        response = rds.start_db_instance(DBInstanceIdentifier=RDS_INSTANCE_ID)
        app.log.info(f"RDS {RDS_INSTANCE_ID} iniciado com sucesso. Resposta: {response}")
        return {'status': 'success', 'action': 'start'}
    except rds.exceptions.InvalidDBInstanceStateFault:
        app.log.error(f"Instância {RDS_INSTANCE_ID} já está em execução ou em estado inválido")
    except Exception as e:
        app.log.error(f"Erro ao iniciar RDS: {str(e)}")
        raise

@app.schedule(Cron(0, 1, '*', '*', '?', '*'))  # 01:00 UTC = 22:00 Brasília (UTC-3)
def stop_rds(event):
    """Para a instância RDS diariamente às 22:00 (horário de Brasília)."""
    rds = boto3.client('rds', region_name=AWS_REGION)
    try:
        response = rds.stop_db_instance(DBInstanceIdentifier=RDS_INSTANCE_ID)
        app.log.info(f"RDS {RDS_INSTANCE_ID} parado com sucesso. Resposta: {response}")
        return {'status': 'success', 'action': 'stop'}
    except rds.exceptions.InvalidDBInstanceStateFault:
        app.log.error(f"Instância {RDS_INSTANCE_ID} já está parada ou em estado inválido")
    except Exception as e:
        app.log.error(f"Erro ao parar RDS: {str(e)}")
        raise   
