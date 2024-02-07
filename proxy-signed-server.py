import os
from dotenv import load_dotenv, get_key
from db import ProxySignedDB
from mq import ProxySignedMQ
from substrate import Substrate
import threading
import time


class ProxySignedServer:
    def __init__(self):
        load_dotenv()
        self.db = ProxySignedDB(os.getenv('DB_HOST'))
        self.mq = ProxySignedMQ(
            mq_host=os.getenv('RABBIT_HOST'),
            mq_port=os.getenv('RABBIT_PORT'),
            mq_username=os.getenv('RABBIT_USERNAME'),
            mq_password=os.getenv('RABBIT_PASSWORD'),
            exchange_name_list=["proxy"],
            queue_name_list=["proxy_queue"],
        )
        self.substarte = Substrate(
            node_url=os.getenv('SUBSTRATE_NODE'),
            proxy_keypair_mnemonic=os.getenv('PROXY_ACCOUNT_PRIVATE')
        )

    def sub_proxy(self):
        # 订阅json
        self.mq.subscribe_message("proxy_queue", self.receive_batch_calls)

    # 订阅消息callback
    def receive_batch_calls(self, ch, method, properties, body):
        print(" \n[x] Received %r" % body)
        # TODO:处理json -> 入库

    # 未执行交易定时器
    def non_exect_timer(self):
        while True:
            all_nonexec_signeds = self.db.get_all_nonexec_signeds()
            if len(all_nonexec_signeds) > 0:
                for signed in all_nonexec_signeds:
                    item = dict(signed._mapping)
                    self.substarte.tx_proxy_announce(item.get("call_hash"))
            time.sleep(6)

    # 检查交易结果定时器
    def check_proxy_announced_timer(self):
        while True:
            all_nonexec_signeds = self.db.get_all_nonexec_signeds()
            if len(all_nonexec_signeds) > 0:
                for signed in all_nonexec_signeds:
                    item = dict(signed._mapping)
                    self.substarte.get_proxy_announced(
                        [item.get("account")], item.get("call_hash"))
                    # TODO:处理结果 -> 入库 -> 发送消息
            time.sleep(6)


if __name__ == "__main__":
    # 每个线程需要有自己独立的连接池
    thread1 = threading.Thread(target=ProxySignedServer().sub_proxy)
    thread1.start()
    thread2 = threading.Thread(target=ProxySignedServer().non_exect_timer)
    thread2.start()
    thread3 = threading.Thread(
        target=ProxySignedServer().check_proxy_announced_timer)
    thread3.start()
