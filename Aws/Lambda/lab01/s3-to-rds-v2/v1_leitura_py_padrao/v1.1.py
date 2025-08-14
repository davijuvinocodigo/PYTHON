import boto3
import os
import json
import logging
from datetime import datetime

# Configurações
def get_config():
    return {
        's3': {
            'input_prefix': 'entrada/',
            'processed_prefix': 'processados/',
            'error_prefix': 'erros/'
        }
    }

def read_and_move_s3_file(bucket_name, file_key, config):
    s3 = boto3.client('s3')
    try:
        
        # Validar prefixo de entrada
        if not file_key.startswith(config['s3']['input_prefix']):
            return {
                'status': 'error',
                'error': f"Arquivo não está no diretório de entrada: {config['s3']['input_prefix']}",
                'file': file_key
            }
        
        # Fazer download e exibir conteúdo
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        
        # Exibir dados do arquivo
        print("Conteúdo do arquivo:")
        print(content)
        
        # Obter data atual para adicionar ao nome do arquivo
        current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Extrair nome do arquivo sem o prefixo
        file_name = file_key.split('/')[-1]
        
        # Criar novo nome com data de processamento
        processed_file_name = f"{file_name.split('.')[0]}_{current_date}.{file_name.split('.')[1]}"
        
        # Mover arquivo para processados com novo nome
        new_key = file_key.replace(
            config['s3']['input_prefix'], 
            config['s3']['processed_prefix']
        ).replace(file_name, processed_file_name)
        
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': file_key},
            Key=new_key
        )
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        
        return {
            'status': 'success',
            'message': 'Arquivo lido e movido com sucesso',
            'original_content_length': len(content),
            'file': new_key
        }
        
    except Exception as e:
        logging.error(f"Falha na leitura do arquivo: {str(e)}")
        try:
            # Mover para erro em caso de falha
            error_key = file_key.replace(
                config['s3']['input_prefix'], 
                config['s3']['error_prefix']
            )
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=error_key
            )
            s3.delete_object(Bucket=bucket_name, Key=file_key)
        except Exception as move_error:
            logging.error(f"Falha ao mover arquivo para erro: {str(move_error)}")
        
        return {
            'status': 'error',
            'error': str(e),
            'file': file_key
        }

# Handler principal do Lambda
def lambda_handler(event, context):
    config = get_config()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Processar cada registro do evento
    for record in event['Records']:
        s3_info = record['s3']
        bucket_name = s3_info['bucket']['name']
        file_key = s3_info['object']['key']
        
        logger.info(f"Processando novo arquivo: {bucket_name}/{file_key}")
        result = read_and_move_s3_file(bucket_name, file_key, config)
        logger.info(f"Resultado do processamento: {json.dumps(result)}")
    
    return {
        'statusCode': 200,
        'body': 'Processamento concluído'
    }