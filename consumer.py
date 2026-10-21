import json
import time
from collections import defaultdict
from kafka import KafkaConsumer
from database import init_db, salvar_transacao

# Inicializa o banco de dados
init_db()

consumer = KafkaConsumer(
    'transacoes',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Memória local para análise de regras por cliente: {cliente_id: [transacao1, transacao2, ...]}
historico_clientes = defaultdict(list)

def limpar_historico_antigo(historico, tempo_limite_segundos=600):
    """Remove transações com mais de 10 minutos (600s) da memória."""
    agora = time.time()
    return [t for t in historico if agora - t['timestamp'] <= tempo_limite_segundos]

def verificar_fraudes(transacao):
    cliente_id = transacao['cliente_id']
    agora = transacao['timestamp']
    
    # Atualiza o histórico do cliente
    historico = historico_clientes[cliente_id]
    historico.append(transacao)
    historico = limpar_historico_antigo(historico)
    historico_clientes[cliente_id] = historico
    
    fraudes = []
    
    # Case 1: ALTO_VALOR (valor >= 10000)
    if transacao['valor'] >= 10000:
        fraudes.append("ALTO_VALOR")
        
    # Case 2: TEMPO_60s (Mesmo cliente com >= 4 transações nos últimos 60 segundos)
    txs_60s = [t for t in historico if agora - t['timestamp'] <= 60]
    if len(txs_60s) >= 4:
        fraudes.append("TEMPO_60s")
        
    # Case 3: GEO_10m (Mesmo cliente com transações em cidades diferentes nos últimos 10 minutos / 600s)
    cidades_10m = set(t['cidade'] for t in historico if agora - t['timestamp'] <= 600)
    if len(cidades_10m) >= 2:
        fraudes.append("GEO_10m")
        
    return fraudes

print("--- Consumidor de Transações / Detecção de Fraudes Iniciado ---")

for message in consumer:
    tx = message.value
    fraudes = verificar_fraudes(tx)
    
    # Salva todas as transações no SQLite
    salvar_transacao(tx, fraudes)
    
    # Exibe no console
    time_str = time.strftime('%H:%M:%S', time.localtime(tx['timestamp']))
    print(f"\n[PROCESSADO {time_str}] Cliente: {tx['cliente_id']} | Valor: R$ {tx['valor']:.2f} | Cidade: {tx['cidade']}")
    
    if fraudes:
        print(f"  🚨 ALERTA DE FRAUDE DETECTADA: {', '.join(fraudes)}")
    else:
        print("  ✅ Transação normal.")
