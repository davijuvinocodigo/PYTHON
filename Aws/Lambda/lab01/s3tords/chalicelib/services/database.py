import pymysql
from pymysql.cursors import DictCursor
from typing import List
from chalicelib.core.config import DBConfig
from chalicelib.core.exceptions import DatabaseError
from chalicelib.core.logger import logger

class DatabaseService:
    def __init__(self, config: DBConfig):
        self.config = config
    
    def _get_connection(self):
        try:
            return pymysql.connect(
                host=self.config.host,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                port=self.config.port,
                cursorclass=DictCursor
            )
        except Exception as e:
            logger.error(f"Erro de conexão: {str(e)}")
            raise DatabaseError(f"Falha na conexão: {str(e)}")
    
    def bulk_upsert(self, table: str, columns: List[str], data: List[tuple], batch_size: int = 1000):
        """Insere/atualiza dados em lote"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cols = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            updates = ', '.join([f"{c}=VALUES({c})" for c in columns])
            
            query = f"""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
            """
            
            for i in range(0, len(data), batch_size):
                cursor.executemany(query, data[i:i + batch_size])
                conn.commit()
                logger.info(f"Lote {i//batch_size + 1} persistido")
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na persistência: {str(e)}")
            raise DatabaseError(f"Falha na persistência: {str(e)}")
        finally:
            cursor.close()
            conn.close()