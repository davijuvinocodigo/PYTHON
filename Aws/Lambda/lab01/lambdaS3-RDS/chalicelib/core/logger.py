import logging

def configurar_log():
    """Configuração básica de logging"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()

    formato = logging.Formatter('%(levelname)s - %(message)s')
    
    handler = logging.StreamHandler()
    handler.setFormatter(formato)
    logger.addHandler(handler)
    
    return logger

log = configurar_log()