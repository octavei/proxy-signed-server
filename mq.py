import pika


class ProxySignedMQ:
    def __init__(self, mq_host="localhost", mq_port=5672, exchange_name_list=["proxy"], queue_name_list=["proxy_queue"]):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(mq_host, mq_port))
        self.channel = self.connection.channel()
        self.exchange_name_list = exchange_name_list
        self.queue_name_list = queue_name_list
        for exchange in exchange_name_list:
            self.channel.exchange_declare(
                exchange=exchange, exchange_type='direct')
        for queue_name in queue_name_list:
            self.channel.queue_declare(queue=queue_name)
        self.channel.start_consuming()

    # 订阅消息
    def subscribe_message(self, queue_name: str,  callback):
        if queue_name not in self.queue_name_list:
            raise Exception(
                f"Invalid value for 'queue_name': {queue_name}. Allowed values are {self.queue_name_list}.")
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

    # 发送消息
    def send_message(self, exchange: str, routing_key: str, body: str):
        if exchange not in self.exchange_name_list:
            raise Exception(
                f"Invalid value for 'exchange': {exchange}. Allowed values are {self.exchange_name_list}.")
        if routing_key not in self.queue_name_list:
            raise Exception(
                f"Invalid value for 'routing_key': {routing_key}. Allowed values are {self.queue_name_list}.")
        self.channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body)
