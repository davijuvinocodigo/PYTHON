from chalicelib.core.exceptions import ErroArmazenamento, ErroArquivoInvalido
from chalicelib.core.logger import log
from datetime import datetime

class GerenciadorArquivos:
    def __init__(self, config):
        """
        Args:
            config: ConfigArmazenamento - configurações de paths
        """
        self.config = config
    
    def validar_local_arquivo(self, caminho: str) -> bool:
        """Valida se o arquivo está no diretório correto"""
        if not caminho.startswith(self.config.entrada):
            raise ErroArquivoInvalido(
                f"Arquivo deve estar em {self.config.entrada}"
            )
        return True
    
    def ler_conteudo(self, caminho: str) -> str:
        """Método genérico para ler conteúdo de arquivo"""
        raise NotImplementedError("Deve ser implementado pela subclasse")
    
    def mover_arquivo(self, origem: str, sucesso: bool) -> str:
        """Método genérico para mover arquivo"""
        raise NotImplementedError("Deve ser implementado pela subclasse")

class GerenciadorS3(GerenciadorArquivos):
    def __init__(self, config):
        super().__init__(config)
        import boto3
        self.cliente = boto3.client('s3')
    
    
    def ler_conteudo(self, bucket: str, caminho: str) -> str:
        """Lê conteúdo de arquivo no S3"""
        try:
            self.validar_local_arquivo(caminho)
            resposta = self.cliente.get_object(
                Bucket=bucket,
                Key=caminho
            )
            return resposta['Body'].read().decode('utf-8')
        except Exception as e:
            log.error(f"Falha ao ler arquivo: {str(e)}")
            raise ErroArmazenamento(f"Erro ao ler arquivo: {str(e)}")
    

    def mover_arquivo(self, bucket: str, origem: str, sucesso: bool) -> str:
        """Move arquivo entre pastas no S3 com a data e hora atual no nome"""
        try:
            destino_base = origem.replace(
                self.config.entrada,
                self.config.processados if sucesso else self.config.erros
            )
            
            # Adiciona a data e hora atual ao nome do arquivo
            file_name = origem.split('/')[-1]
            current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
            processed_file_name = f"{file_name.split('.')[0]}_{current_date}.{file_name.split('.')[1]}"
            destino = '/'.join(destino_base.split('/')[:-1] + [processed_file_name])
            
            self.cliente.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': origem},
                Key=destino
            )
            self.cliente.delete_object(Bucket=bucket, Key=origem)
            return destino
        except Exception as e:
            log.error(f"Falha ao mover arquivo: {str(e)}")
            raise ErroArmazenamento(f"Erro ao mover arquivo: {str(e)}")
        
    