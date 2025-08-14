import boto3
import json
import logging


def read_and_move_s3_file(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        # Fazer download e exibir conteúdo
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        
        print("Conteúdo do arquivo:")
        print(content)
        
        return {'status': 'success' }
        
    except Exception as e:
        logging.error(f"Falha na leitura do arquivo: {str(e)}")
        return {
            'status': 'error',
        }

# Handler principal do Lambda
def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Processar cada registro do evento
    for record in event['Records']:
        s3_info = record['s3']
        bucket_name = s3_info['bucket']['name']
        file_key = s3_info['object']['key']
        
        logger.info(f"Processando novo arquivo: {bucket_name}/{file_key}")
        result = read_and_move_s3_file(bucket_name, file_key)
        logger.info(f"Resultado do processamento: {json.dumps(result)}")
    
    return {
        'statusCode': 200,
        'body': 'Processamento concluído'
    }