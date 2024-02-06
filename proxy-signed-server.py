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
            queue_name_list=["proxy_queue"],
            exchange_name_list=["proxy"]
        )
        self.substarte = Substrate(
            node_url=os.getenv('SUBSTRATE_NODE'),
            proxy_keypair_mnemonic=os.getenv('PROXY_ACCOUNT_PRIVATE')
        )

    def run(self):
        # 订阅json
        self.mq.subscribe_message("proxy_queue", self.receive_batch_calls)

        # 未执行交易轮询
        self.non_exect_timer()

        # # 获取交易执行结果轮询
        self.check_proxy_announced_timer()

        # non_exect_thread = threading.Thread(target=self.non_exect_timer)
        # non_exect_thread.daemon = True
        # non_exect_thread.start()

        # check_proxy_announced_thread = threading.Thread(
        #     target=self.check_proxy_announced_timer)
        # check_proxy_announced_thread.daemon = True
        # check_proxy_announced_thread.start()

    # 订阅消息callback
    def receive_batch_calls(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
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
    server = ProxySignedServer()
    server.run()
