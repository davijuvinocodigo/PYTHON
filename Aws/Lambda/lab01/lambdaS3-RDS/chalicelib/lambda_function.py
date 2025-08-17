from typing import Dict, Any
from .core.config import ConfigApp, carregar_mapeamento
from .core.logger import log
from .services.armazenamento import GerenciadorS3
from .services.db import GerenciadorMySQL
from .services.processador import ProcessadorArquivo

class ProcessadorHandler:
    """Orquestrador principal do processamento"""
    
    def __init__(self):
        self.config = ConfigApp()
        self.mapeamento = carregar_mapeamento()
        self.armazenamento = GerenciadorS3(self.config.storage)
        self.db = GerenciadorMySQL(self.config.db)
        self.processador = ProcessadorArquivo(
            self.mapeamento,
            self.config.processor.delimitador
        )
    
    def executar(self, evento: Dict) -> Dict[str, Any]:
        """Método principal para execução do processamento"""
        resultados = []
        
        for registro in evento.get('Records', []):
            try:
                resultado = self._processar_registro(registro)
                resultados.append({
                    'status': 'sucesso',
                    **resultado
                })
            except Exception as e:
                log.error(f"Erro ao processar registro: {str(e)}")
                resultados.append({
                    'status': 'erro',
                    'erro': str(e),
                    'arquivo': registro.get('s3', {}).get('object', {}).get('key')
                })
        
        return {
            'statusCode': 200,
            'body': {
                'processados': len([r for r in resultados if r['status'] == 'sucesso']),
                'erros': len([r for r in resultados if r['status'] == 'erro']),
                'detalhes': resultados
            }
        }
    
    def _processar_registro(self, registro: Dict) -> Dict:
        """Processa um registro individual do evento"""
        bucket = registro['s3']['bucket']['name']
        arquivo = registro['s3']['object']['key']
        
        log.info(f"Iniciando processamento de {bucket}/{arquivo}")
        
        log.info(f"1. Obter arquivo.....")
        conteudo = self.armazenamento.ler_conteudo(bucket, arquivo)
        
        log.info(f"2. Processar dados..........")
        dados = self.processador.processar(conteudo)

        #log.info(f"Dados para persistencia: {dados}")
        #log.info(f"3. Persistir no banco..............")
        #persistido = self.db.persistir(dados, self.mapeamento, self.config.db.tamanho_lote)
        
        
        log.info(f"4. Mover arquivo............................")
        novo_caminho = self.armazenamento.mover_arquivo(
            bucket, arquivo, sucesso=True
        )
        
        return {
            'arquivo': novo_caminho,
            'registros_processados': {t: len(r) for t, r in dados.items()}
        }

def lambda_handler(event, context):
    handler = ProcessadorHandler()
    return handler.executar(event)