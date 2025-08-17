class ErroProcessamento(Exception):
    """Classe base para erros de processamento"""
    pass

class ErroArmazenamento(ErroProcessamento):
    """Erros relacionados ao armazenamento de arquivos"""
    pass

class ErroBancoDados(ErroProcessamento):
    """Erros relacionados ao banco de dados"""
    pass

class ErroArquivoInvalido(ErroProcessamento):
    """Arquivo inválido ou em local incorreto"""
    pass

class ErroTransformacaoDados(ErroProcessamento):
    """Falha ao transformar dados"""
    pass