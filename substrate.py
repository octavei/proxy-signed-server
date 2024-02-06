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

        # TODO:获取最终块的高度,判断proxy_keypair
        # self.api.get_chain_finalised_head()

    # 获得签名的结果
    def get_proxy_announced(self, accountIds: list(), call_hash: str):
        return self.api.query(
            module='proxy',
            storage_function='announcements',
            params=accountIds,
            block_hash=call_hash
        )

    # 获取callhash
    def get_proxy_announce_call_hash(self, real, callHash):
        tx_proxy_announce = self.api.compose_call(
            call_module='proxy',
            call_function='announce',
            call_params={
                'real': self.proxy_keypair.ss58_address,
                'real': callHash
            })

        tx_proxy_announce_hex = tx_proxy_announce.encode().to_hex()
        tx_proxy_announce_call_hash = blake2b(tx_proxy_announce_hex.encode(
            "utf-8"), digest_size=32).hexdigest()
        return tx_proxy_announce_call_hash

    # 发起交易
    def tx_proxy_announce(self, call_hash):
        extrinsic = self.api.create_signed_extrinsic(
            call=call_hash, keypair=self.proxy_keypair)
        try:
            receipt = self.api.submit_extrinsic(
                extrinsic, wait_for_inclusion=True)
            print("Extrinsic '{}' sent and included in block '{}'".format(
                receipt.extrinsic_hash, receipt.block_hash))

        except SubstrateRequestException as e:
            print("Failed to send: {}".format(e))
