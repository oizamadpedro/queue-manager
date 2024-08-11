import pika
import time
import json
from main import Queue

def callback(ch, method, properties, body):
    print(f" [x] Recebido {body}")

    decoded_string = body.decode('utf-8')
    json_string = decoded_string.replace("'", '"')
    data = json.loads(json_string)

    process_id = data.get('process_id')

    queue_response = Queue().processing(process_id)

    response = create_transaction(body)  # Simula o processamento

    completed_queue = Queue().completed(process_id)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def create_transaction(body):
    print(f"Received: {body}")
    time.sleep(2)
    return {
        "message": "success"
    }

def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    print(' [*] Aguardando mensagens. Para sair, pressione CTRL+C')
    channel.start_consuming()

# Para iniciar o consumidor, você pode executar este script diretamente:
if __name__ == "__main__":
    start_consuming()