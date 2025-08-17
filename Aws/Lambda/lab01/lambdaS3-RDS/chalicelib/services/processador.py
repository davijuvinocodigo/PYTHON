from typing import List, Dict, Any
from chalicelib.core.logger import log
from chalicelib.services.transformar import TransformadorDados



class ProcessadorArquivo:
    def __init__(self, mapeamento: List[Dict], delimitador: str = ';'):
        """
        Args:
            mapeamento: Lista de dicionários com configuração das tabelas/colunas
            delimitador: Delimitador do arquivo (padrão: ;)
        """
        self.mapeamento = mapeamento
        self.delimitador = delimitador
        self.transformador = TransformadorDados()
    
    def processar(self, conteudo: str) -> Dict[str, List[tuple]]:
        """Processa conteúdo do arquivo e retorna dados estruturados"""
        import csv
        from io import StringIO
        
        leitor = csv.reader(StringIO(conteudo), delimiter=self.delimitador)
        dados = {tabela['tabela']: [] for tabela in self.mapeamento}
        
        for num_linha, linha in enumerate(leitor, 1):
            if not linha:
                log.warning(f"Linha {num_linha} vazia - ignorando")
                continue
            
            for tabela in self.mapeamento:
                valores = []
                for coluna in tabela['colunas']:
                    try:
                        valor = self._obter_valor_coluna(linha, num_linha, coluna)
                        valores.append(valor)
                    except Exception as e:
                        log.error(f"Erro na coluna {coluna['nome']}, linha {num_linha}: {str(e)}")
                        valores.append(None)
                
                dados[tabela['tabela']].append(tuple(valores))
        
        return dados
    
    def _obter_valor_coluna(self, linha: List[str], num_linha: int, coluna: Dict[str, Any]) -> Any:
        """Obtém valor processado para uma coluna específica"""
        origem = coluna['origem']
        
        if origem['tipo'] == 'coluna':
            valor = linha[origem['index']] if origem['index'] < len(linha) else None
        elif origem['tipo'] == 'constante':
            valor = origem['valor']
        elif origem['tipo'] == 'funcao':
            valor = None  # Será transformado
        else:
            valor = None
        
        if valor is not None and 'transformacao' in origem:
            valor = self.transformador.aplicar_transformacao(valor, origem['transformacao'])
        elif origem['tipo'] == 'funcao':
            valor = self.transformador.aplicar_transformacao(None, origem['valor'])
        
        return valor