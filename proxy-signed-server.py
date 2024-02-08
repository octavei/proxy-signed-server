import os
from dotenv import load_dotenv
import datetime
from db import ProxySignedDB
from substrate import Substrate
import threading
import time


class ProxySignedServer:
    def __init__(self):
        load_dotenv()
        self.db = ProxySignedDB(os.getenv('DB_HOST'))
        self.substarte = Substrate(
            node_url=os.getenv('SUBSTRATE_NODE'),
            proxy_keypair_mnemonic=os.getenv('PROXY_ACCOUNT_PRIVATE')
        )

    # 处理call
    def call(self, call_json):
        try:
            call_hash = self.substarte.get_call_hash(call=call_json)
        except Exception as e:
            raise Exception("Illegal transaction")
        if self.db.get_signed(call_hash) is not None:
            raise Exception("Duplicate transaction")

        insert_data = {
            "call": call_json,
            "call_hash": call_hash,
            "status": 0,
            "create_time": datetime.datetime.now()
        }
        try:
            with self.db.session.begin():
                self.db.insert_or_update_signed([insert_data])
        except Exception as e:
            raise e

    # 签名并提交
    def sign_and_tx_timer(self):
        while True:
            all_no_sign = self.db.get_all_no_sign()
            if len(all_no_sign) > 0:
                with self.db.session.begin():
                    for no_sign in all_no_sign:
                        item = dict(no_sign._mapping)

                        # 1.给未签名的call签名
                        if item.get("sign") is None:
                            sign = self.substarte.tx_proxy_announce_sign(
                                item.get("call_hash"))
                            item["sign"] = sign
                            self.db.insert_or_update_signed([item])

                        # 2.没有代理权限
                        proxies = self.substarte.get_proxy_proxies()
                        if proxies is None:
                            item["status"] = 3
                            item["reason"] = "No Proxy permission"
                            self.db.insert_or_update_signed([item])
                            break

                        # 3.重复交易
                        announcements = self.substarte.get_proxy_announcements(
                            item.get("call_hash"))
                        if announcements is not None:
                            item["status"] = 3
                            item["reason"] = "Duplicate transaction"
                            self.db.insert_or_update_signed([item])
                            break

                        # 4.提交
                        try:
                            send_result = self.substarte.tx_proxy_announce_sign_send(
                                item.get("sign"))
                            if send_result is not None:
                                item["status"] = 1
                                item["exec_height"] = send_result.get(
                                    "block_num") + proxies.get("deploy")
                                self.db.insert_or_update_signed([item])
                        except Exception as e:
                            print(
                                f"=====sign_and_tx_timer error=======\n{e}\n==============")
                            raise e

            time.sleep(6)

    # 执行交易
    def exec_tx_timer(self):
        while True:
            now_height = self.substarte.get_last_block_num()
            all_can_exec = self.db.get_all_can_exec(now_height)
            if len(all_can_exec) > 0:
                with self.db.session.begin():
                    for can_exec in all_can_exec:
                        item = dict(can_exec._mapping)

                        # 1.交易不存在
                        announcements = self.substarte.get_proxy_announcements(
                            item.get("call_hash"))
                        if announcements is None:
                            item["status"] = 3
                            item["reason"] = "Rejected or already executed"
                            self.db.insert_or_update_signed([item])
                            break

                        # 2.没有代理权限
                        proxies = self.substarte.get_proxy_proxies()
                        if proxies is None:
                            item["status"] = 3
                            item["reason"] = "No Proxy permission"
                            self.db.insert_or_update_signed([item])
                            break

                        # 3.提交
                        try:
                            send_result = self.substarte.tx_proxy_announce_sign_send(
                                item.get("sign"))
                            if send_result is not None:
                                item["status"] = 2
                                item["tx_hash"] = send_result.get("tx_hash")
                                item["block_num"] = send_result.get(
                                    "block_num")
                                item["tx_id"] = send_result.get("tx_id")
                                item["block_hash"] = send_result.get(
                                    "block_hash")
                                self.db.insert_or_update_signed([item])
                            else:
                                item["status"] = 3
                                item["reason"] = send_result.get("message")
                                self.db.insert_or_update_signed([item])
                        except Exception as e:
                            print(
                                f"=====exec_tx_timer error=======\n{e}\n==============")
                            raise e
            time.sleep(6)


if __name__ == "__main__":
    # 每个线程需要有自己独立的连接池
    thread1 = threading.Thread(
        target=ProxySignedServer().sign_and_tx_timer)
    thread1.start()
    thread2 = threading.Thread(
        target=ProxySignedServer().exec_tx_timer)
    thread2.start()
