import boto3
from chalicelib.core.config import S3Config
from chalicelib.core.exceptions import StorageError, InvalidFileError
from chalicelib.core.logger import logger

class StorageService:
    def __init__(self, config: S3Config):
        self.config = config
        self.client = boto3.client('s3')
    
    def get_file(self, bucket: str, key: str) -> str:
        """Obtém conteúdo do arquivo"""
        try:
            if not key.startswith(self.config.input_prefix):
                raise InvalidFileError(f"Arquivo deve estar em {self.config.input_prefix}")
                
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f"Erro ao ler arquivo: {str(e)}")
            raise StorageError(f"Falha ao ler arquivo: {str(e)}")
    
    def move_file(self, bucket: str, key: str, success: bool) -> str:
        """Move arquivo para processados/erros"""
        try:
            if success:
                new_key = key.replace(self.config.input_prefix, self.config.processed_prefix)
            else:
                new_key = key.replace(self.config.input_prefix, self.config.error_prefix)
            
            self.client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=new_key
            )
            self.client.delete_object(Bucket=bucket, Key=key)
            return new_key
        except Exception as e:
            logger.error(f"Erro ao mover arquivo: {str(e)}")
            raise StorageError(f"Falha ao mover arquivo: {str(e)}")