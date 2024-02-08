from substrateinterface import SubstrateInterface, Keypair
from hashlib import blake2b


class Substrate:
    def __init__(self, node_url: str, proxy_keypair_mnemonic: str):
        self.api = SubstrateInterface(
            url=node_url,
            ss58_format=42,
            type_registry_preset='substrate-node-template'
        )
        self.proxy_keypair = Keypair.create_from_mnemonic(
            proxy_keypair_mnemonic)

    # 获取最新区块高度
    def get_last_block_num(self):
        block_hash = self.api.get_chain_finalised_head()
        return self.api.get_block_number(block_hash)

    # 用于判断proxy_keypair的权限，或者获取proxy_keypair的信息(delay)
    def get_proxy_proxies(self):
        block_hash = self.api.get_chain_finalised_head()
        (info, height) = self.api.query(
            module='Proxy',
            storage_function='Proxies',
            params=[self.proxy_keypair.ss58_address],
            block_hash=block_hash
        )
        info_list = list(info)
        if len(info_list) > 0:
            return dict(info_list[0])
        else:
            return None

    # 计算callhash
    def get_call_hash(self, call: dict):
        tx = self.api.compose_call(
            call_module=call.get("module"),
            call_function=call.get("method"),
            call_params=call.get("params")
        )

        tx_hex = tx.encode().to_hex()
        tx_call_hash = blake2b(tx_hex.encode(
            "utf-8"), digest_size=32).hexdigest()
        return tx_call_hash

    # 获取代理的结果
    def get_proxy_announcements(self, call_hash: str):
        (info, height) = self.api.query(
            module='Proxy',
            storage_function='Announcements',
            params=[self.proxy_keypair.ss58_address],
            block_hash=call_hash
        )
        info_list = list(info)
        if len(info_list) > 0:
            return dict(info_list[0])
        else:
            return None

    # 代理交易签名
    def tx_proxy_announce_sign(self, call_hash):
        call = substrate.compose_call(
            call_module='Proxy',
            call_function='Announce',
            call_params={
                'real': self.proxy_keypair.ss58_address,
                'callHash': call_hash
            }
        )
        sign = self.api.create_signed_extrinsic(
            call=call, keypair=self.proxy_keypair)
        return sign

     # 代理交易签名交易
    def tx_proxy_announce_sign_send(self, sign):
        try:
            receipt = self.api.submit_extrinsic(
                sign, wait_for_inclusion=True)
            # TODO:处理并返回结果
            # {
            #     "tx_hash": "",
            #     "block_num": "",
            #     "tx_id": "",
            #     "block_hash": "",
            #     "message": ""  # 失败时存储消息
            # }
            return receipt
        except SubstrateRequestException as e:
            raise e
