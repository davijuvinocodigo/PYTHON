import json
import boto3
import os

def lambda_handler(event, context):
    # Recupera o nome do bucket e do arquivo do evento S3
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    # Cria uma instância do cliente do S3
    s3_client = boto3.client('s3')
    # Lê o conteúdo do arquivo do S3
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        # Processa o conteúdo do arquivo (exemplo: escreve no console)
        print(f"Arquivo {file_name} do bucket {bucket_name} foi processado")
        print(f"Conteúdo do arquivo:\n{content}")
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
    return {
        'statusCode': 200,
        'body': json.dumps('Arquivo processado com sucesso')
    }