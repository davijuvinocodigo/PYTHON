import os
from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class ConfigDB:
    host: str = os.getenv('DB_HOST')
    user: str = os.getenv('DB_USER')
    password: str = os.getenv('DB_PASSWORD')
    database: str = os.getenv('DB_NAME')
    port: int = int(os.getenv('DB_PORT', 3306))

@dataclass
class ConfigGerenciador:
    entrada: str = 'entrada/'
    processados: str = 'processados/'
    erros: str = 'erros/'

@dataclass
class ConfigProcessador:
    delimitador: str = ';'
    tamanho_lote: int = int(os.getenv('BATCH_SIZE', 1000))

@dataclass
class ConfigApp:
    db: ConfigDB = field(default_factory=ConfigDB)
    storage: ConfigGerenciador = field(default_factory=ConfigGerenciador)
    processor: ConfigProcessador = field(default_factory=ConfigProcessador)


def carregar_mapeamento() -> List[Dict[str, Any]]:
    """Carrega configuração de mapeamento de colunas"""
    # Pode ser substituído por leitura de JSON, banco de dados, etc.
    '''
    {
        'tabela': 'exemplo',
        'colunas': [
            {'nome': 'id', 'origem': {'tipo': 'coluna', 'index': 0}},
            {'nome': 'nome', 'origem': {'tipo': 'coluna', 'index': 1}},
            {'nome': 'data', 'origem': {'tipo': 'coluna', 'index': 2, 'transformacao': 'data'}}
        ]
    }
    '''
    return [
        
        {
            'tabela': 'tbv9088_regr_prod_plar',
            'colunas': [
                {'nome': 'cod_regr_prod_plar', 'origem': {'tipo': 'coluna', 'index': 4}},
                {'nome': 'nom_regr_prod_plar', 'origem': {'tipo': 'coluna', 'index': 7}},
                {'nome': 'des_regr_prod_plar', 'origem': {'tipo': 'coluna', 'index': 7}},
                {'nome': 'ind_rgto_ativ', 'origem': {'tipo': 'constante', 'valor': 'S'}},
                {'nome': 'dat_hor_inio_vige__regr_prod', 'origem': {'tipo': 'coluna', 'index': 2, 'transformacao': 'data'}},
                {'nome': 'dat_hor_usua_atui_rgto', 'origem': {'tipo': 'funcao', 'valor': 'dataatul'}},
                {'nome': 'num_funl_cola_cogl_atud', 'origem': {'tipo': 'constante', 'valor': '000000000'}}
            ]
        },
        {
            'tabela': 'tbv9086_carc_regr_prod_plar',
            'colunas': [
                {'nome': 'cod_regr_prod_plar', 'origem': {'tipo': 'coluna', 'index': 4}},
                {'nome': 'cod_tipo_carc_espo_prod', 'origem': {'tipo': 'coluna', 'index': 0, 'transformacao': 'caracteres'}},
                {'nome': 'cod_carc_espo_prod_plar', 'origem': {'tipo': 'coluna', 'index': 1}},
                {'nome': 'dat_hor_usua_atui_rgto', 'origem': {'tipo': 'funcao', 'valor': 'dataatul'}},
                {'nome': 'num_funl_cola_cogl_atud', 'origem': {'tipo': 'constante', 'valor': '000000000'}}
            ]
        }
    ]