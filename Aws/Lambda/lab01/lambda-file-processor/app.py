import boto3
import os
import csv
from io import StringIO
from datetime import datetime
from chalice import Chalice

app = Chalice(app_name='lambda-file-processor')

# Configurações
SOURCE_BUCKET = 'dev-bucket-lab01'
DESTINATION_PREFIX = 'processados/'
ERROR_PREFIX = 'erros/'
AWS_REGION = 'us-east-1'

s3_client = boto3.client('s3', region_name=AWS_REGION)

@app.on_s3_event(bucket=SOURCE_BUCKET, events=['s3:ObjectCreated:*'])
def handle_new_file(event):
    file_key = event.key
    
    # Verifica se é um arquivo CSV
    if not file_key.lower().endswith('.csv'):
        app.log.debug(f"Ignorando arquivo não CSV: {file_key}")
        return {
            'status': 'skipped',
            'message': 'Arquivo não é CSV',
            'file': file_key
        }
    
    try:
        # 1. Ler o arquivo CSV do S3
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        app.log.debug(f"Iniciando processamento do arquivo: {file_key}")
        
        # 2. Processar o arquivo CSV
        processed_data, headers = process_csv_file(csv_content, file_key)
        
        # 3. Gerar relatório de processamento
        report_content = generate_processing_report(processed_data, headers, file_key)
        upload_report(report_content, file_key)
        
        # 4. Mover o arquivo para a pasta de processados com data
        destination_key = generate_destination_key(file_key)
        move_file(SOURCE_BUCKET, file_key, destination_key)
        
        return {
            'status': 'success',
            'original_file': file_key,
            'processed_file': destination_key,
            'timestamp': datetime.now().isoformat(),
            'rows_processed': len(processed_data)
        }
    except Exception as e:
        app.log.error(f"Erro ao processar arquivo {file_key}: {str(e)}")
        # Move o arquivo para a pasta de erros em caso de falha
        error_key = f"{ERROR_PREFIX}{datetime.now().strftime('%Y-%m-%d')}/{os.path.basename(file_key)}"
        move_file(SOURCE_BUCKET, file_key, error_key)
        
        return {
            'status': 'error',
            'error': str(e),
            'file': file_key,
            'error_location': error_key
        }

def process_csv_file(csv_content, filename):
    """Processa o conteúdo CSV e retorna os dados e cabeçalhos"""
    try:
        # Lê o CSV usando o módulo csv
        reader = csv.reader(StringIO(csv_content))
        headers = next(reader)  # Pega a primeira linha (cabeçalhos)
        
        # Converte cabeçalhos para minúsculas
        headers = [header.strip().lower() for header in headers]
        
        # Processa as linhas
        processed_data = []
        for row in reader:
            if len(row) != len(headers):
                app.log.warning(f"Linha ignorada - número de colunas diferente: {row}")
                continue
            
            # Cria um dicionário com os dados
            row_data = dict(zip(headers, row))
            
            # Adiciona timestamp de processamento
            row_data['processed_at'] = datetime.now().isoformat()
            
            # Exemplo de validação/filtro (adaptar conforme necessidade)
            if 'valor' in row_data and not row_data['valor'].strip():
                app.log.debug(f"Linha com valor vazio: {row_data}")
                continue
            
            processed_data.append(row_data)
        
        app.log.debug(f"CSV processado - Linhas: {len(processed_data)}, Colunas: {headers}")
        return processed_data, headers
    
    except csv.Error as e:
        app.log.error(f"Erro ao ler CSV: {str(e)}")
        raise ValueError(f"Formato do arquivo CSV inválido: {str(e)}")
    except StopIteration:
        app.log.error("Arquivo CSV vazio ou sem cabeçalhos")
        raise ValueError("Arquivo CSV está vazio ou sem cabeçalhos")

def generate_processing_report(data, headers, original_filename):
    """Gera um relatório em formato CSV com estatísticas do processamento"""
    report = [
        ["Arquivo original", original_filename],
        ["Data processamento", datetime.now().isoformat()],
        ["Total registros processados", len(data)],
        ["Colunas", ", ".join(headers)],
        ["Exemplo registro", str(data[0]) if data else "N/A"]
    ]
    
    # Cria o conteúdo CSV do relatório
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Métrica", "Valor"])  # Cabeçalho do relatório
    writer.writerows(report)
    
    return output.getvalue()

def upload_report(report_content, original_filename):
    """Faz upload do relatório para o S3"""
    report_filename = f"relatorios/{datetime.now().strftime('%Y-%m-%d')}/{os.path.splitext(os.path.basename(original_filename))[0]}_report.csv"
    
    s3_client.put_object(
        Bucket=SOURCE_BUCKET,
        Key=report_filename,
        Body=report_content.encode('utf-8')
    )
    
    app.log.debug(f"Relatório de processamento salvo em: {report_filename}")

def generate_destination_key(original_key):
    """Gera a chave de destino com data"""
    filename = os.path.basename(original_key)
    current_date = datetime.now().strftime('%Y-%m-%d')
    return f"{DESTINATION_PREFIX}{current_date}/{filename}"

def move_file(bucket, source_key, destination_key):
    """Move o arquivo no S3 copiando e depois deletando o original"""
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': source_key},
        Key=destination_key
    )
    s3_client.delete_object(Bucket=bucket, Key=source_key)
    app.log.debug(f"Arquivo movido de {source_key} para {destination_key}")