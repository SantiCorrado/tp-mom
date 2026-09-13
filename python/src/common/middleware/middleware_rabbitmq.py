import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.connect()
        
    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)


    def start_consuming(self, on_message_callback):
        def callback(ch, method, _, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def start_consuming(self, on_message_callback):
        def callback(ch, method, _, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        for routing_key in self.routing_keys:
            self.channel.queue_declare(queue=self.queue_name, exclusive=True)
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        for routing_key in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

    def close(self):
        self.connection.close()