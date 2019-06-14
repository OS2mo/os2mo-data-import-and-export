import os
import pika
import xmltodict

AMQP_USER = os.environ.get('AMQP_USER')
AMQP_PASSWORD = os.environ.get('AMQP_PASSWORD', None)
VIRTUAL_HOST = os.environ.get('VIRTUAL_HOST', None)
if not (AMQP_USER and AMQP_PASSWORD and VIRTUAL_HOST):
    raise Exception('Credentials missing')

credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASSWORD)
parameters = pika.ConnectionParameters(host='msg-amqp.silkeborgdata.dk',
                                       port=5672,
                                       virtual_host=VIRTUAL_HOST,
                                       credentials=credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.exchange_declare(exchange='callback-topic', exchange_type='topic')

result = channel.queue_declare('callback', exclusive=False, durable=True)
queue_name = result.method.queue

channel.queue_bind(exchange='callback-topic', queue=queue_name, routing_key='#')
print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
        xml_response = xmltodict.parse(body)
        print(xml_response)

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
