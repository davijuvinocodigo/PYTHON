from chalice import Chalice
import boto3
import requests
import json
from datetime import datetime

app = Chalice(app_name='lambda-rest-chalice')


# Configurações
S3_BUCKET = 'dev-bucket-lab01'
API_URL = 'https://api.mercadobitcoin.net/api/v4/XLM/networks'


@app.route('/trigger')
def trigger_lambda():
    try:
        # 1. Fazer requisição para a API
        response = requests.get(API_URL)
        response.raise_for_status()  # Levanta exceção para erros HTTP
        
        data = response.json()
        
        # 2. Criar nome único para o arquivo no S3
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        s3_key = f'api-data/{timestamp}.json'
        
        # 3. Conectar ao S3 e fazer upload do arquivo
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': f'Dados da API salvos com sucesso em s3://{S3_BUCKET}/{s3_key}'
        }
    
    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'body': f'Erro ao acessar a API: {str(e)}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Erro inesperado: {str(e)}'
        }


