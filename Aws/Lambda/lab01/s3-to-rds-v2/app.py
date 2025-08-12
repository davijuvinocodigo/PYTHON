from chalice import Chalice
import mysql.connector
import boto3
import csv
import io
import json
import os
import logging
from datetime import datetime

app = Chalice(app_name='file-processor')

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
            'bucket': os.getenv('S3_BUCKET', 'dev-bucket-lab01'),
            'input_prefix': 'entrada/',
            'processed_prefix': 'processados/',
            'error_prefix': 'erros/'
        },
        'batch_size': int(os.getenv('BATCH_SIZE', 1000)),
        'mapping': {
            'file_delimiter': ';',
            'tables': [
                {
                    'name': 'tbv9088_regr_prod_plar',
                    'columns': [
                        {'name': 'cod_regr_prod_plar', 'source': {'type': 'column', 'index': 4}},
                        {'name': 'nom_regr_prod_plar', 'source': {'type': 'column', 'index': 7}},
                        {'name': 'des_regr_prod_plar', 'source': {'type': 'column', 'index': 7}},
                        {'name': 'ind_rgto_ativ', 'source': {'type': 'constant', 'value': 'S'}},
                        {'name': 'dat_hor_inio_vige__regr_prod', 'source': {'type': 'column', 'index': 2, 'transform': 'date'}},
                        {'name': 'dat_hor_usua_atui_rgto', 'source': {'type': 'function', 'value': 'now'}},
                        {'name': 'num_funl_cola_cogl_atud', 'source': {'type': 'constant', 'value': '000000000'}}
                    ]
                },
                {
                    'name': 'tbv9086_carc_regr_prod_plar',
                    'columns': [
                        {'name': 'cod_regr_prod_plar', 'source': {'type': 'column', 'index': 4}},
                        {'name': 'cod_tipo_carc_espo_prod', 'source': {'type': 'column', 'index': 0, 'transform': 'ippi'}},
                        {'name': 'cod_carc_espo_prod_plar', 'source': {'type': 'column', 'index': 1}},
                        {'name': 'dat_hor_usua_atui_rgto', 'source': {'type': 'function', 'value': 'now'}},
                        {'name': 'num_funl_cola_cogl_atud', 'source': {'type': 'constant', 'value': '000000000'}}
                    ]
                }
            ]
        }
    }

# Transformações
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

# Processamento de arquivo
def process_file(content, config):
    logging.info("Processando arquivo CSV")
    reader = csv.reader(io.StringIO(content), delimiter=config['mapping']['file_delimiter'])
    rows = list(reader)
    table_data = {table['name']: [] for table in config['mapping']['tables']}
    
    for row_idx, row in enumerate(rows, 1):
        try:
            # Verifica se a linha tem conteúdo válido
            if not row or len(row) == 0:
                logging.warning(f"Linha {row_idx} vazia - ignorando")
                continue
                
            for table in config['mapping']['tables']:
                values = []
                for col in table['columns']:
                    source = col['source']
                    try:
                        if source['type'] == 'column':
                            # Verifica se o índice existe na linha
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
                        values.append(None)  # Usa None como fallback
                
                table_data[table['name']].append(tuple(values))
        except Exception as e:
            logging.error(f"Erro ao processar linha {row_idx}: {str(e)}")
            continue
    
    return table_data

# Banco de dados (restante do código permanece igual)
def save_to_db(table_data, config):
    try:
        conn = mysql.connector.connect(**config['db'])
        cursor = conn.cursor()
        
        for table_name, data in table_data.items():
            if not data:
                continue
                
            columns = [col['name'] for table in config['mapping']['tables'] 
                      if table['name'] == table_name for col in table['columns']]
            
            query = f"""
                INSERT INTO {config['db']['database']}.{table_name} ({', '.join(columns)})
                VALUES ({', '.join(['%s']*len(columns))})
                ON DUPLICATE KEY UPDATE {', '.join(f"{col}=VALUES({col})" for col in columns)}
            """
            
            for i in range(0, len(data), config['batch_size']):
                cursor.executemany(query, data[i:i+config['batch_size']])
                logging.info(f"Persistido lote {i//config['batch_size'] + 1}")
        
        conn.commit()
        logging.info("Dados persistidos com sucesso")
        return True
    except Exception as e:
        logging.error(f"Erro na persistência: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()

            

# S3 (restante do código permanece igual)
def handle_s3_file(file_key, config):
    s3 = boto3.client('s3')
    
    try:
        # Obter arquivo
        response = s3.get_object(Bucket=config['s3']['bucket'], Key=file_key)
        content = response['Body'].read().decode('utf-8')
        
        # Processar
        table_data = process_file(content, config)
        
        # Salvar no banco
        save_to_db(table_data, config)
        
        # Mover arquivo
        new_key = file_key.replace(config['s3']['input_prefix'], config['s3']['processed_prefix'])
        s3.copy_object(
            Bucket=config['s3']['bucket'],
            CopySource={'Bucket': config['s3']['bucket'], 'Key': file_key},
            Key=new_key
        )
        s3.delete_object(Bucket=config['s3']['bucket'], Key=file_key)
        
        return {
            'status': 'success',
            'processed_tables': {k: len(v) for k, v in table_data.items()},
            'file': new_key
        }
    except Exception as e:
        logging.error(f"Falha no processamento: {str(e)}")
        try:
            error_key = file_key.replace(config['s3']['input_prefix'], config['s3']['error_prefix'])
            s3.copy_object(
                Bucket=config['s3']['bucket'],
                CopySource={'Bucket': config['s3']['bucket'], 'Key': file_key},
                Key=error_key
            )
            s3.delete_object(Bucket=config['s3']['bucket'], Key=file_key)
        except Exception as move_error:
            logging.error(f"Falha ao mover arquivo para erro: {str(move_error)}")
        
        return {
            'status': 'error',
            'error': str(e),
            'file': file_key
        }

# Handler do Chalice
@app.on_s3_event(
    bucket=get_config()['s3']['bucket'],
    events=['s3:ObjectCreated:*'],
    prefix=get_config()['s3']['input_prefix']
)
def handle_s3_event(event):
    config = get_config()
    return handle_s3_file(event.key, config)