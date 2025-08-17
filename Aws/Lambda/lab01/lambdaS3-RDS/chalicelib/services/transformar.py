from datetime import datetime
from typing import Any


class TransformadorDados:
    """Responsável por todas as transformações de dados"""
    
    @staticmethod
    def aplicar_transformacao(valor: Any, tipo: str) -> Any:
        """Aplica transformação conforme tipo especificado"""
        transformacoes = {
            'data': TransformadorDados._transformar_data,
            'caracteres': TransformadorDados._transformar_caracteres,
            'dataatul': TransformadorDados._transformar_data_atual
        }
        
        if tipo in transformacoes:
            return transformacoes[tipo](valor)
        return valor
    
    @staticmethod
    def _transformar_data(valor: str) -> str:
        try:
            partes = valor.split('.')
            return f"{partes[2]}-{partes[1]}-{partes[0]} 00:00:00"
        except Exception:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def _transformar_caracteres(valor: str) -> int:
        return 1 if valor and valor.upper() in ['C'] else 2
    
    @staticmethod
    def _transformar_data_atual(_: Any) -> str:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')