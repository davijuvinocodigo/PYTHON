from chalice import Chalice
import mysql.connector
import boto3
import csv
import io
from datetime import datetime
import os
import logging

# Configuração inicial do logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO if os.environ.get('DEBUG', 'False').lower() == 'true' else logging.WARNING)

app = Chalice(app_name='rds-csv-processor')
app.debug = os.environ.get('DEBUG', 'False').lower() == 'true'

# Configurations
S3_BUCKET = os.environ.get('S3_BUCKET', 'dev-bucket-lab01')
INPUT_PREFIX = 'entrada/'
PROCESSED_PREFIX = 'processados/'
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'aws')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1000'))
CURRENT_USER = '000000000'

class RDSConnectionManager:
    @staticmethod
    def get_connection():
        try:
            logger.debug("Estabelecendo conexão com o banco de dados RDS")
            conn = mysql.connector.connect(
                host=os.environ['DB_HOST'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                database=DB_SCHEMA,
                port=3306
            )
            logger.info("Conexão com o RDS estabelecida com sucesso")
            return conn
        except Exception as e:
            logger.error(f"Falha ao conectar ao banco de dados: {str(e)}")
            raise

    @classmethod
    def lambda_handler(cls, event, context):
        s3 = boto3.client('s3')
        file_key = event.key
        
        try:
            logger.info(f"Iniciando processamento do arquivo: {file_key}")
            
            # Get file from S3
            logger.debug(f"Obtendo arquivo {file_key} do S3")
            response = s3.get_object(Bucket=S3_BUCKET, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')
            logger.debug(f"Arquivo {file_key} obtido com sucesso, tamanho: {len(csv_content)} bytes")
            
            # Process data
            logger.debug("Iniciando parse do CSV")
            usuarios, documentos = CSVProcessor.parse_csv_content(csv_content)
            logger.info(f"CSV parseado com sucesso - {len(usuarios)} usuários e {len(documentos)} documentos encontrados")
            
            cls.process_data(usuarios, documentos)
            logger.info("Dados processados e inseridos no banco de dados com sucesso")
            
            # Move processed file
            new_key = file_key.replace(INPUT_PREFIX, PROCESSED_PREFIX, 1)
            logger.debug(f"Movendo arquivo para {new_key}")
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={'Bucket': S3_BUCKET, 'Key': file_key},
                Key=new_key
            )
            s3.delete_object(Bucket=S3_BUCKET, Key=file_key)
            logger.info(f"Arquivo movido para {new_key} e removido da pasta de entrada")
            
            return {
                'status': 'success',
                'processed_rows': len(usuarios),
                'file': new_key
            }
        except Exception as e:
            logger.error(f"Erro ao processar {file_key}: {str(e)}", exc_info=True)
            raise

    @classmethod
    def process_data(cls, usuarios, documentos):
        conn = None
        try:
            logger.debug("Iniciando processamento dos dados no banco de dados")
            conn = cls.get_connection()
            conn.autocommit = False
            
            with conn.cursor() as cursor:
                # Process usuarios
                user_query = f"""
                    INSERT INTO {DB_SCHEMA}.Usuario 
                    (id_usuario, nome, descricao, ativo, data_ini, data_use_atu, usu) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        nome = VALUES(nome),
                        descricao = VALUES(descricao),
                        ativo = VALUES(ativo),
                        data_ini = VALUES(data_ini),
                        data_use_atu = VALUES(data_use_atu),
                        usu = VALUES(usu)
                """
                
                total_users = len(usuarios)
                logger.info(f"Inserindo/atualizando {total_users} usuários em lotes de {BATCH_SIZE}")
                
                for i in range(0, total_users, BATCH_SIZE):
                    batch = usuarios[i:i+BATCH_SIZE]
                    logger.debug(f"Processando lote de usuários {i+1}-{min(i+BATCH_SIZE, total_users)}")
                    cursor.executemany(user_query, batch)
                
                # Process documentos
                doc_query = f"""
                    INSERT INTO {DB_SCHEMA}.Documento 
                    (id_documento, id_usuario, numero_documento, data, usu, tipo) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        id_usuario = VALUES(id_usuario),
                        numero_documento = VALUES(numero_documento),
                        data = VALUES(data),
                        usu = VALUES(usu),
                        tipo = VALUES(tipo)
                """
                
                total_docs = len(documentos)
                logger.info(f"Inserindo/atualizando {total_docs} documentos em lotes de {BATCH_SIZE}")
                
                for i in range(0, total_docs, BATCH_SIZE):
                    batch = documentos[i:i+BATCH_SIZE]
                    logger.debug(f"Processando lote de documentos {i+1}-{min(i+BATCH_SIZE, total_docs)}")
                    cursor.executemany(doc_query, batch)
            
            conn.commit()
            logger.info("Transação commitada com sucesso")
            
        except Exception as e:
            logger.error(f"Erro durante o processamento dos dados: {str(e)}", exc_info=True)
            if conn:
                conn.rollback()
                logger.warning("Transação revertida devido a erro")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
                logger.debug("Conexão com o banco de dados fechada")

class CSVProcessor:
    @staticmethod
    def convert_date(date_str):
        try:
            day, month, year = date_str.split('.')
            return f"{year}-{month}-{day} 00:00:00"
        except Exception as e:
            logger.warning(f"Data inválida '{date_str}', usando data atual. Erro: {str(e)}")
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def convert_ippi(ippi_str):
        try:
            result = 1 if ippi_str.upper() == 'C' else 2
            logger.debug(f"Convertido IPPI '{ippi_str}' para {result}")
            return result
        except Exception as e:
            logger.warning(f"Valor IPPI inválido '{ippi_str}', usando padrão 2. Erro: {str(e)}")
            return 2

    @staticmethod
    def parse_csv_content(csv_content):
        logger.debug("Iniciando parse do conteúdo CSV")
        csv_data = list(csv.reader(io.StringIO(csv_content), delimiter=';'))
        usuarios = []
        documentos = []
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Processando {len(csv_data)} linhas do CSV")
        
        for idx, row in enumerate(csv_data, 1):
            try:
                if len(row) >= 8:
                    ippi = CSVProcessor.convert_ippi(row[0])
                    usuarios.append((
                        row[4],       # id_usuario
                        row[7],       # nome
                        row[7],       # descricao
                        'N',          # ativo
                        CSVProcessor.convert_date(row[2]),  # data_ini
                        now,          # data_use_atu
                        CURRENT_USER  # usu
                    ))
                    documentos.append((
                        row[1],       # id_documento
                        row[4],       # id_usuario
                        'rg-111111',  # numero_documento
                        now,          # data
                        CURRENT_USER, # usu
                        ippi          # tipo
                    ))
                else:
                    logger.warning(f"Linha {idx} ignorada - número insuficiente de colunas: {row}")
            except Exception as e:
                logger.error(f"Erro ao processar linha {idx}: {row}. Erro: {str(e)}")
                continue
        
        logger.info(f"Parse concluído - {len(usuarios)} usuários e {len(documentos)} documentos válidos")
        return usuarios, documentos

@app.on_s3_event(bucket=S3_BUCKET,
                 events=['s3:ObjectCreated:*'],
                 prefix=INPUT_PREFIX)
def handle_s3_event(event):
    logger.info(f"Evento S3 recebido para o arquivo: {event.key}")
    try:
        result = RDSConnectionManager.lambda_handler(event, None)
        logger.info("Processamento concluído com sucesso")
        return result
    except Exception as e:
        logger.critical(f"Falha crítica no processamento: {str(e)}", exc_info=True)
        raise