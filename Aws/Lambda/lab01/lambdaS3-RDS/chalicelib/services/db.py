from chalicelib.core.exceptions import ErroBancoDados
from chalicelib.core.logger import log
from typing import List
import pymysql

class GerenciadorBanco:
    """Classe base genérica para operações de banco de dados"""
    def __init__(self, config):
        self.config = config
    
    def conectar(self):
        """Método para estabelecer conexão"""
        raise NotImplementedError("Deve ser implementado pela subclasse")
    
    def inserir_lote(self, tabela: str, colunas: List[str], dados: List[tuple]):
        """Insere dados em lote de forma genérica"""
        raise NotImplementedError("Deve ser implementado pela subclasse")

class GerenciadorMySQL(GerenciadorBanco):
    def conectar(self):
        """Implementação específica para MySQL"""
        try:
            return pymysql.connect(
                host=self.config.host,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                port=self.config.port
            )
        except Exception as e:
            log.error(f"Erro de conexão: {str(e)}")
            raise ErroBancoDados(f"Falha na conexão: {str(e)}")
    
    def inserir_lote(self, tabela: str, colunas: List[str], dados: List[tuple], tamanho_lote: int = 1000):
        """Implementação de upsert em lote para MySQL"""
        conexao = self.conectar()
        cursor = conexao.cursor()
        
        try:
            cols = ', '.join(colunas)
            placeholders = ', '.join(['%s'] * len(colunas))
            updates = ', '.join([f"{c}=VALUES({c})" for c in colunas])
            
            query = f"""
                INSERT INTO {tabela} ({cols})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
            """
            
            for i in range(0, len(dados), tamanho_lote):
                cursor.executemany(query, dados[i:i + tamanho_lote])
                conexao.commit()
                log.info(f"Lote {i//tamanho_lote + 1} persistido")
                
        except Exception as e:
            conexao.rollback()
            log.error(f"Erro na persistência: {str(e)}")
            raise ErroBancoDados(f"Falha na persistência: {str(e)}")
        finally:
            cursor.close()
            conexao.close()



    def persistir(self, dados: List[tuple], mapeamento: str ,tamanho_lote: int) -> bool:
        try:
            for tabela, registros in dados.items():
                if not registros:
                    continue
                
                colunas = [
                    col['nome'] for col in 
                    next(t for t in mapeamento if t['tabela'] == tabela)['colunas']
                ]
                
                self.inserir_lote(tabela, colunas, registros, tamanho_lote)
            log.info("Dados persistidos com sucesso")
            return True
        except Exception as e:
            log.error(f"Erro ao persistir dados: {str(e)}")
            raise ErroBancoDados(f"Falha ao persistir dados: {str(e)}")        