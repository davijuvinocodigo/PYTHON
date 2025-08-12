import random
from datetime import datetime, timedelta

def generate_test_data(num_records=1000):
    # Inicializa conjuntos para garantir unicidade
    used_irrp = set()
    used_codrpp = set()
    
    # Lista para armazenar os registros
    records = []
    
    for i in range(1, num_records + 1):
        # Gera IRRP único (8 dígitos com zeros à esquerda)
        while True:
            irrp = f"{random.randint(1, 99999999):08d}"
            if irrp not in used_irrp:
                used_irrp.add(irrp)
                break
                
        # Gera CODRPP único (3 dígitos)
        while True:
            codrpp = f"{random.randint(100, 999)}"
            if codrpp not in used_codrpp:
                used_codrpp.add(codrpp)
                break
        
        # IPPI aleatório (C ou I)
        ippi = random.choice(['C', 'I'])
        
        # Datas aleatórias dentro de um intervalo de 5 anos
        base_date = datetime.now()
        start_date = base_date + timedelta(days=random.randint(1, 365*5))
        end_date = start_date + timedelta(days=random.randint(1, 365))
        proc_date = start_date + timedelta(days=random.randint(1, 30))
        
        # Formata datas como dd.mm.yyyy
        data_ini = start_date.strftime('%d.%m.%Y')
        data_fim = end_date.strftime('%d.%m.%Y')
        data_proc = proc_date.strftime('%d.%m.%Y')
        
        # CODPRO aleatório (3 dígitos)
        codpro = f"{random.randint(100, 999)}"
        
        # Texto de exemplo
        text = f"text_{random.randint(1, 100)}"
        
        # Cria o registro
        record = f"{ippi};{irrp};{data_ini};{data_fim};{codrpp};{codpro};{data_proc};{text}"
        records.append(record)
    
    return records

# Gera 1000 registros
test_data = generate_test_data(200)

# Salva em arquivo CSV
with open('dados_teste.csv', 'w') as f:
    f.write('\n'.join(test_data))

print("Arquivo 'dados_teste.csv' gerado com sucesso com 1000 registros únicos.")
print("Exemplo das primeiras linhas:")
print('\n'.join(test_data[:2]))