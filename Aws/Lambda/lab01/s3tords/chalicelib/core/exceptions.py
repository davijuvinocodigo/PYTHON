class ProcessingError(Exception):
    """Base para erros de processamento"""
    pass

class StorageError(ProcessingError):
    """Erros relacionados ao armazenamento"""
    pass

class DatabaseError(ProcessingError):
    """Erros relacionados ao banco de dados"""
    pass

class InvalidFileError(ProcessingError):
    """Arquivo inválido ou em local incorreto"""
    pass