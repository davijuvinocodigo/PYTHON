import boto3
import csv
import io
import os
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configurações
def get_config():
    return {
        'db': {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_SCHEMA'),
            'port': 3306
        },
        's3': {
            'input_prefix': 'entrada/',
            'processed_prefix': 'processados/',
            'error_prefix': 'erros/'
        },
        'batch_size': int(os.getenv('BATCH_SIZE', 1000)),
        'mapping': {
            'file_delimiter': ';',
            'tables': [
                # ... (manter seu mapeamento original aqui)
            ]
        }
    }

# Transformações (mantido igual)
def transform_value(value, transform_type):
    if transform_type == 'date':
        try:
            day, month, year = value.split('.')
            return f"{year}-{month}-{day} 00:00:00"
        except:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    elif transform_type == 'ippi':
        return 1 if value and value.upper() == 'C' else 2
    elif transform_type == 'now':
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return value

# Processamento de arquivo (mantido igual)
def process_file(content, config):
    logging.info("Processando arquivo CSV")
    reader = csv.reader(io.StringIO(content), delimiter=config['mapping']['file_delimiter'])
    rows = list(reader)
    table_data = {table['name']: [] for table in config['mapping']['tables']}
    for row_idx, row in enumerate(rows, 1):
        try:
            if not row or len(row) == 0:
                logging.warning(f"Linha {row_idx} vazia - ignorando")
                continue
            for table in config['mapping']['tables']:
                values = []
                for col in table['columns']:
                    source = col['source']
                    try:
                        if source['type'] == 'column':
                            if source['index'] >= len(row):
                                logging.warning(f"Índice {source['index']} fora do range na linha {row_idx} - usando valor padrão")
                                value = None
                            else:
                                value = row[source['index']]
                        elif source['type'] == 'constant':
                            value = source['value']
                        elif source['type'] == 'function':
                            value = transform_value(None, source['value'])
                        else:
                            value = None
                        if value is not None and 'transform' in source:
                            value = transform_value(value, source['transform'])
                        values.append(value)
                    except Exception as col_error:
                        logging.error(f"Erro ao processar coluna {col['name']} na linha {row_idx}: {str(col_error)}")
                        values.append(None)
                table_data[table['name']].append(tuple(values))
        except Exception as e:
            logging.error(f"Erro ao processar linha {row_idx}: {str(e)}")
            continue
    return table_data

# Banco de dados com SQLAlchemy (mantido igual)
def get_engine(config):
    db = config['db']
    connection_string = (
        f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    )
    return create_engine(connection_string)

def save_to_db(table_data, config):
    engine = get_engine(config)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for table_name, data in table_data.items():
            if not data:
                continue
            columns = [col['name'] for table in config['mapping']['tables'] 
                      if table['name'] == table_name for col in table['columns']]
            insert_stmt = f"""
                INSERT INTO {config['db']['database']}.{table_name} ({', '.join(columns)})
                VALUES ({', '.join([':{}'.format(col) for col in columns])})
                ON DUPLICATE KEY UPDATE {', '.join(f"{col}=VALUES({col})" for col in columns)}
            """
            for i in range(0, len(data), config['batch_size']):
                batch = data[i:i+config['batch_size']]
                dict_batch = [
                    {col: v for col, v in zip(columns, row)} for row in batch
                ]
                session.execute(text(insert_stmt), dict_batch)
                logging.info(f"Persistido lote {i//config['batch_size'] + 1}")
        session.commit()
        logging.info("Dados persistidos com sucesso")
        return True
    except Exception as e:
        logging.error(f"Erro na persistência: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


# Função de processamento do S3
def process_s3_file(bucket_name, file_key, config):
    s3 = boto3.client('s3')
    try:
        # Validar prefixo de entrada
        if not file_key.startswith(config['s3']['input_prefix']):
            return {
                'status': 'error',
                'error': f"Arquivo não está no diretório de entrada: {config['s3']['input_prefix']}",
                'file': file_key
            }

        # Download e processamento
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        #table_data = process_file(content, config)
        #save_to_db(table_data, config)
        
        # Obter data atual para adicionar ao nome do arquivo
        current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = file_key.split('/')[-1]
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
        
        return {
            'status': 'success',
            'processed_tables': {k: len(v) for k, v in table_data.items()},
            'file': new_key
        }
        
    except Exception as e:
        logging.error(f"Falha no processamento: {str(e)}")
        try:
            # Mover para erro
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
        result = process_s3_file(bucket_name, file_key, config)
        logger.info(f"Resultado do processamento: {json.dumps(result)}")
    
    return {
        'statusCode': 200,
        'body': 'Processamento concluído'
    }        