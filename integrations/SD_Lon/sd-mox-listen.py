import os
import time
import pika
import html
import xmltodict


AMQP_USER = os.environ.get('AMQP_USER')
AMQP_PASSWORD = os.environ.get('AMQP_PASSWORD', None)
VIRTUAL_HOST = os.environ.get('VIRTUAL_HOST', None)
if not (AMQP_USER and AMQP_PASSWORD and VIRTUAL_HOST):
    raise Exception('Credentials missing')


def callback(ch, method, properties, body):
    body = str(body)[2:-1]
    body = html.unescape(body)
    body = body.replace('\\t', '\t')
    body = body.replace('\\r', '\n')
    body = body.replace('\\n', '')
    xml_response = xmltodict.parse(body)
    print(xml_response)


credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASSWORD)
parameters = pika.ConnectionParameters(host='msg-amqp.silkeborgdata.dk',
                                       port=5672,
                                       virtual_host=VIRTUAL_HOST,
                                       credentials=credentials)

print('Listning to queue')
while True:
    try:
        t = time.time()

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.exchange_declare(exchange='callback-topic', exchange_type='topic')

        result = channel.queue_declare('callback', exclusive=False, durable=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='callback-topic', queue=queue_name,
                           routing_key='#')

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True
        )
        channel.start_consuming()
    except pika.exceptions.StreamLostError:
        print('Restart: {}s'.format(time.time() - t))
        t = time.time()
