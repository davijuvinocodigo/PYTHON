from chalice import Chalice
import boto3
import requests
import json
from datetime import datetime, timedelta 
import logging
from tempfile import NamedTemporaryFile
from typing import List, Union





logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


app = Chalice(app_name='lambda-rest-chalice')

@app.route('/trigger')
def trigger_lambda():
    try:
        # Chama a função handler diretamente (simulando invocação)
        result = handler({}, None)
        
        return {
            "status": "success",
            "message": "Lambda executada com sucesso!",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}







def handler(event, context):
    try:
        # 1. Fazer requisição para a API
        coin = "BTC"  # Defina a moeda desejada
        date = (datetime.now() - timedelta(days=1)).date()
        data = MercadoBitcoinApi(coin=coin).get_data(date=date)
        
        # 2. Criar nome único para o arquivo no S3
        writer = S3Writer(coin=coin)
        writer.write(data)
        
        return {
            'statusCode': 200,
            'body': f'Dados da API salvos com sucesso. Arquivo: {writer.key}'
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









# 1. Fazer requisição para a API
class MercadoBitcoinApi:
    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.base_endpoint = "https://www.mercadobitcoin.net/api"

    def _get_endpoint(self, date: datetime.date) -> str:
        return f"{self.base_endpoint}/{self.coin}/day-summary/{date.year}/{date.month}/{date.day}"

    def get_data(self, date: datetime.date) -> dict:
        endpoint = self._get_endpoint(date=date)
        logger.info(f"Getting data from endpoint: {endpoint}")
        response = requests.get(endpoint)
        return response.json()
    









# 2. Criar nome único para o arquivo no S3
class S3Writer:
    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.tempfile = NamedTemporaryFile()
        self.key = f"mercado_bitcoin/day-summary/coin={self.coin}/extracted_at={datetime.now().date()}/{datetime.now()}.json"
        self.s3 = boto3.client("s3")

    def _write_to_file(self, data: Union[List, dict]):
        with open(self.tempfile.name, "a") as f:
            f.write(json.dumps(data) + "\n")

    def _write_file_to_s3(self):
        self.s3.put_object(
            Body=self.tempfile,
            Bucket="dev-bucket-lab01",
            Key=self.key
        )

    def write(self, data: Union[List, dict]):
        self._write_to_file(data=data)
        self._write_file_to_s3()
