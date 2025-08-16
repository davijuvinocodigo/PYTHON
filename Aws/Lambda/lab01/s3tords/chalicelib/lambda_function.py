import json
from chalicelib.core.config import AppConfig, TABLE_MAPPINGS
from chalicelib.core.exceptions import ProcessingError
from chalicelib.core.logger import logger
from chalicelib.services.storage import StorageService
from chalicelib.services.database import DatabaseService
from chalicelib.services.processor import DataProcessor

def lambda_handler(event, context):
    
    # Configurações
    config = AppConfig()
    
    # Inicializa serviços
    storage = StorageService(config.s3)
    database = DatabaseService(config.db)
    processor = DataProcessor(TABLE_MAPPINGS)
    
    results = []
    
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            # 1. Obter arquivo
            content = storage.get_file(bucket, key)
            
            # 2. Processar dados
            table_data = processor.process_csv(content)
            
            # 3. Salvar no banco
            for table_name, data in table_data.items():
                if not data:
                    continue
                
                columns = [col['name'] for col in next(t for t in TABLE_MAPPINGS if t['name'] == table_name)['columns']]      
                database.bulk_upsert(table=table_name, columns=columns, data=data, batch_size=config.batch_size)

            # 4. Mover arquivo para processados
            new_key = storage.move_file(bucket, key, success=True)
            
            results.append({
                'status': 'success',
                'file': new_key,
                'processed': {k: len(v) for k, v in table_data.items()}
            })
            
        except ProcessingError as e:
            logger.error(f"Erro no processamento: {str(e)}")
            new_key = storage.move_file(bucket, key, success=False)
            results.append({
                'status': 'error',
                'file': new_key,
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': {
            'processed_files': len([r for r in results if r['status'] == 'success']),
            'failed_files': len([r for r in results if r['status'] == 'error']),
            'details': results
        }
    }