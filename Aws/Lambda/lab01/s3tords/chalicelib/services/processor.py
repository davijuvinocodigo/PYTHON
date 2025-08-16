import csv
import io
from datetime import datetime
from typing import Dict, List, Any
from chalicelib.core.logger import logger

class DataProcessor:
    def __init__(self, mappings: List[Dict], delimiter: str = ';'):
        self.mappings = mappings
        self.delimiter = delimiter
    
    def transform_value(self, value: Any, transform_type: str) -> Any:
        """Aplica transformações aos valores"""
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
    
    def process_csv(self, content: str) -> Dict[str, List[tuple]]:
        """Processa conteúdo CSV e retorna dados estruturados por tabela"""
        logger.info("Processando arquivo CSV")
        reader = csv.reader(io.StringIO(content), delimiter=self.delimiter)
        table_data = {table['name']: [] for table in self.mappings}
        
        for row_idx, row in enumerate(reader, 1):
            if not row:
                logger.warning(f"Linha {row_idx} vazia - ignorando")
                continue
            
            for table in self.mappings:
                values = []
                for col in table['columns']:
                    try:
                        value = self._get_column_value(row, row_idx, col)
                        values.append(value)
                    except Exception as e:
                        logger.error(f"Erro na coluna {col['name']}, linha {row_idx}: {str(e)}")
                        values.append(None)
                
                table_data[table['name']].append(tuple(values))
        
        return table_data
    
    def _get_column_value(self, row: List[str], row_idx: int, col: Dict[str, Any]) -> Any:
        """Obtém valor de uma coluna aplicando transformações se necessário"""
        source = col['source']
        
        if source['type'] == 'column':
            value = row[source['index']] if source['index'] < len(row) else None
        elif source['type'] == 'constant':
            value = source['value']
        elif source['type'] == 'function':
            value = self.transform_value(None, source['value'])
        else:
            value = None
        
        if value is not None and 'transform' in source:
            value = self.transform_value(value, source['transform'])
        
        return value