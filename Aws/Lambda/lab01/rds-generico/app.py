from chalice import Chalice
import mysql.connector
import os
import boto3
import csv
import io
from datetime import datetime
from typing import List, Tuple, Dict, Any

app = Chalice(app_name='rds-generico')
app.debug = os.environ.get('DEBUG', 'False').lower() == 'true'

# Configurations
S3_BUCKET = os.environ.get('S3_BUCKET', 'dev-bucket-lab01')
S3_KEY = os.environ.get('S3_KEY', 'dados.csv')
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'aws')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1000'))
CURRENT_USER = '000000000'

class DatabaseManager:
    @staticmethod
    def get_connection() -> mysql.connector.connection.MySQLConnection:
        try:
            return mysql.connector.connect(
                host=os.environ['DB_HOST'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                database=DB_SCHEMA,
                port=3306
            )
        except Exception as e:
            app.log.error(f"Database connection error: {str(e)}")
            raise

class S3Service:
    @staticmethod
    def get_file_content(bucket: str, key: str) -> str:
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            app.log.error(f"S3 read error: {str(e)}")
            raise

class CSVProcessor:
    @staticmethod
    def convert_date(date_str: str) -> str:
        """Convert dd.mm.yyyy to yyyy-mm-dd with time"""
        try:
            day, month, year = date_str.split('.')
            return f"{year}-{month}-{day} 00:00:00"
        except:
            return date_str

    @staticmethod
    def convert_ippi(ippi_str: str) -> int:
        """Convert C->1, I->2"""
        return 1 if ippi_str.upper() == 'C' else 2

    @staticmethod
    def parse_csv_content(csv_content: str) -> Tuple[List[Tuple], List[Tuple]]:
        csv_data = list(csv.reader(io.StringIO(csv_content), delimiter=';'))
        usuarios = []
        documentos = []
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for row in csv_data:
            if len(row) >= 8:
                # Extract and transform fields
                ippi = CSVProcessor.convert_ippi(row[0])
                irrp = row[1]
                dataini = CSVProcessor.convert_date(row[2])
                datafim = CSVProcessor.convert_date(row[3])
                codrpp = row[4]
                codpro = row[5]
                dataproc = CSVProcessor.convert_date(row[6])
                text1 = row[7]
                
                # Prepare Usuario data with specified column order
                usuarios.append((
                    codrpp,        # id_usuario
                    text1,         # nome (same as descricao)
                    text1,         # descricao
                    'N',           # ativo
                    dataini,       # data_ini
                    now,           # data_use_atu
                    CURRENT_USER   # usu
                ))
                
                # Prepare Documento data with specified column order
                documentos.append((
                    irrp,          # id_documento
                    codrpp,        # id_usuario
                    'rg-111111',   # numero_documento
                    now,           # data
                    CURRENT_USER,  # usu
                    ippi           # tipo
                ))
        
        return usuarios, documentos

    @classmethod
    def process_in_batches(cls, data: List[Tuple], query: str, cursor: Any) -> None:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            cursor.executemany(query, batch)

class DataService:
    @staticmethod
    def import_csv_data(csv_content: str) -> int:
        conn = None
        try:
            usuarios, documentos = CSVProcessor.parse_csv_content(csv_content)
            
            conn = DatabaseManager.get_connection()
            conn.autocommit = False
            
            with conn.cursor() as cursor:
                # Insert Usuario with specified column order
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
                CSVProcessor.process_in_batches(usuarios, user_query, cursor)
                
                # Insert Documento with specified column order
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
                CSVProcessor.process_in_batches(documentos, doc_query, cursor)
            
            conn.commit()
            return len(usuarios)
            
        except Exception as e:
            if conn:
                conn.rollback()
            app.log.error(f"Data import error: {str(e)}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

@app.route('/')
def index():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.route('/importar-csv', methods=['POST'])
def import_csv():
    try:
        request = app.current_request
        body = request.json_body if request.json_body else {}
        
        bucket = body.get('bucket', S3_BUCKET)
        key = body.get('key', S3_KEY)
        
        csv_content = S3Service.get_file_content(bucket, key)
        rows_processed = DataService.import_csv_data(csv_content)
        
        return {
            "success": True,
            "bucket": bucket,
            "key": key,
            "rows_processed": rows_processed,
            "message": f"Arquivo {key} importado com sucesso"
        }
    except Exception as e:
        return _handle_error(e, "Falha ao importar arquivo CSV")

def _handle_error(exception: Exception, message: str) -> Dict:
    app.log.error(f"{message}: {str(exception)}")
    return {
        "success": False,
        "error": str(exception),
        "message": message
    }, 500