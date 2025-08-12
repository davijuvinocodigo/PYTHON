import csv
from io import StringIO
import boto3
from .database import RDSConnectionManager
from sqlalchemy import text

class CSVProcessor:
    """Processador de arquivos CSV para carga no RDS"""
    
    def __init__(self, db_config, csv_config, s3_client=None):
        self.db_connection = RDSConnectionManager(db_config)
        self.csv_config = csv_config
        self.s3_client = s3_client or boto3.client('s3')
    
    def process_file(self, bucket, key):
        try:
            content = self._get_s3_content(bucket, key)
            result = self._process_content(content)
            
            new_key = key.replace('entrada/', 'processados/')
            self._move_s3_file(bucket, key, new_key)
            
            return {
                'status': 'success',
                'processed': result['processed'],
                'skipped': result['skipped']
            }
        except Exception as e:
            error_key = key.replace('entrada/', 'erros/')
            self._move_s3_file(bucket, key, error_key)
            raise e
    
    def _get_s3_content(self, bucket, key):
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    
    def _process_content(self, content):
        stats = {'processed': 0, 'skipped': 0}
        session = self.db_connection.get_session()
        try:
            reader = csv.reader(
                StringIO(content),
                delimiter=self.csv_config['delimiter'],
                quotechar=self.csv_config.get('quotechar', '"')
            )
            
            batch = []
            for row in reader:
                if not row:
                    continue
                
                if 'field_count' in self.csv_config and len(row) != self.csv_config['field_count']:
                    stats['skipped'] += 1
                    continue
                
                record = {field: row[i].strip() for i, field in enumerate(self.csv_config['fields'])}
                batch.append(record)
                stats['processed'] += 1
                
                if len(batch) >= 100:
                    self._insert_batch(session, batch)
                    batch = []
            
            if batch:
                self._insert_batch(session, batch)
            session.commit()
        finally:
            session.close()
        return stats
    
    def _insert_batch(self, session, batch):
        if not batch:
            return
        
        columns = self.csv_config.get('columns_to_insert', self.csv_config['fields'])
        filtered = [{col: rec[col] for col in columns if col in rec} for rec in batch]
        
        sql = f"INSERT INTO {self.csv_config['table_name']} ({', '.join(columns)}) VALUES ({', '.join(f':{c}' for c in columns)})"
        session.execute(text(sql), filtered)
    
    def _move_s3_file(self, bucket, src, dest):
        self.s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': src},
            Key=dest
        )
        self.s3_client.delete_object(Bucket=bucket, Key=src)