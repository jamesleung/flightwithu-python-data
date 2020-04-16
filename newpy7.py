import pika

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters(host = '122.51.134.114',port = 5672,virtual_host = '/',credentials = credentials))
channel=connection.channel()
result = channel.queue_declare(queue = 'python-test6')

def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print(body.decode())

channel.basic_consume('python-test6',callback)
channel.start_consuming()