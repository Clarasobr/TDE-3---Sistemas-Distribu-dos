import time
import json
import random
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CIDADES = ["São Paulo", "Rio de Janeiro", "Curitiba", "Fortaleza", "Belo Horizonte"]
CLIENTES = [101, 102, 103, 104, 105]

print("--- Produtor de Transações Iniciado ---")

try:
    while True:
        transacao = {
            "id": str(uuid.uuid4()),
            "cliente_id": random.choice(CLIENTES),
            "valor": round(random.uniform(10, 15000), 2),
            "cidade": random.choice(CIDADES),
            "timestamp": time.time()  # Timestamp em segundos
        }
        
        producer.send('transacoes', transacao)
        print(f"[ENVIADO] Cliente {transacao['cliente_id']} | R$ {transacao['valor']} | Cidade: {transacao['cidade']}")
        
        time.sleep(3)
except KeyboardInterrupt:
    print("\nProdutor finalizado.")
