from dataclasses import dataclass, field
import os

@dataclass
class DBConfig:
    host: str = os.getenv('DB_HOST')
    user: str = os.getenv('DB_USER')
    password: str = os.getenv('DB_PASSWORD')
    database: str = os.getenv('DB_SCHEMA')
    port: int = 3306

@dataclass
class S3Config:
    input_prefix: str = 'entrada/'
    processed_prefix: str = 'processados/'
    error_prefix: str = 'erros/'

@dataclass
class AppConfig:
    db: DBConfig = field(default_factory=DBConfig)  
    s3: S3Config = field(default_factory=S3Config)  
    batch_size: int = int(os.getenv('BATCH_SIZE', 1000))

# Mapeamento das colunas (exemplo)
TABLE_MAPPINGS = [
    {
        'name': 'tbv9088_regr_prod_plar',
        'columns': [
            {'name': 'cod_regr_prod_plar', 'source': {'type': 'column', 'index': 4}},
            {'name': 'nom_regr_prod_plar', 'source': {'type': 'column', 'index': 7}},
            {'name': 'des_regr_prod_plar', 'source': {'type': 'column', 'index': 7}},
            {'name': 'ind_rgto_ativ', 'source': {'type': 'constant', 'value': 'S'}},
            {'name': 'dat_hor_inio_vige__regr_prod', 'source': {'type': 'column', 'index': 2, 'transform': 'date'}},
            {'name': 'dat_hor_usua_atui_rgto', 'source': {'type': 'function', 'value': 'now'}},
            {'name': 'num_funl_cola_cogl_atud', 'source': {'type': 'constant', 'value': '000000000'}}
        ]
    },
    {
        'name': 'tbv9086_carc_regr_prod_plar',
        'columns': [
            {'name': 'cod_regr_prod_plar', 'source': {'type': 'column', 'index': 4}},
            {'name': 'cod_tipo_carc_espo_prod', 'source': {'type': 'column', 'index': 0, 'transform': 'ippi'}},
            {'name': 'cod_carc_espo_prod_plar', 'source': {'type': 'column', 'index': 1}},
            {'name': 'dat_hor_usua_atui_rgto', 'source': {'type': 'function', 'value': 'now'}},
            {'name': 'num_funl_cola_cogl_atud', 'source': {'type': 'constant', 'value': '000000000'}}
        ]
    }
]