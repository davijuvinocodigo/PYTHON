from chalice import Chalice
import mysql.connector
import os
import boto3
import csv
import io
from datetime import datetime

app = Chalice(app_name='rds-lambda')
app.debug = True  # Desative em produção

# Configuração inicial
S3_BUCKET = os.environ.get('S3_BUCKET', 'dev-bucket-lab01')
S3_KEY = os.environ.get('S3_KEY', 'dados.csv')
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'aws')

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            port=3306
        )
        return conn
    except Exception as e:
        app.log.error(f"Erro na conexão: {str(e)}")
        raise

def get_s3_file(bucket, key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        app.log.error(f"Erro ao ler arquivo S3: {str(e)}")
        raise

def process_csv_to_rds(csv_content, table_name='Usuario'):
    conn = None
    cursor = None
    try:
        # Ler conteúdo CSV
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        # Conectar ao banco de dados
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Preparar query de inserção
        placeholders = ', '.join(['%s'] * len(csv_reader.fieldnames))
        columns = ', '.join(csv_reader.fieldnames)
        query = f"INSERT INTO {DB_SCHEMA}.{table_name} ({columns}) VALUES ({placeholders})"
        
        # Inserir linhas
        row_count = 0
        for row in csv_reader:
            cursor.execute(query, list(row.values()))
            row_count += 1
        
        conn.commit()
        return row_count
        
    except Exception as e:
        if conn:
            conn.rollback()
        app.log.error(f"Erro ao processar CSV: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

@app.route('/')
def index():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.route('/dados')
def get_data():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(f"SELECT id_usuario, nome FROM {DB_SCHEMA}.Usuario;")
        
        result = cursor.fetchall()
        return {
            "success": True,
            "data": result,
            "count": len(result),
            "message": "Consulta bem-sucedida"
        }
    except mysql.connector.Error as err:
        app.log.error(f"Database error: {err}")
        return {
            "success": False,
            "error": str(err),
            "error_code": err.errno
        }, 500
    except Exception as e:
        app.log.error(f"Unexpected error: {e}")
        return {
            "success": False,
            "error": str(e)
        }, 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route('/importar-csv', methods=['POST'])
def import_csv():
    try:
        # Obter parâmetros do request (opcional)
        request = app.current_request
        body = request.json_body if request.json_body else {}
        
        bucket = body.get('bucket', S3_BUCKET)
        key = body.get('key', S3_KEY)
        table_name = body.get('table_name', 'Usuario')
        
        # Processar arquivo CSV
        csv_content = get_s3_file(bucket, key)
        rows_imported = process_csv_to_rds(csv_content, table_name)
        
        return {
            "success": True,
            "bucket": bucket,
            "key": key,
            "rows_imported": rows_imported,
            "message": f"Arquivo {key} importado com sucesso"
        }
        
    except Exception as e:
        app.log.error(f"Erro na importação: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "message": "Falha ao importar arquivo CSV"
        }, 500
