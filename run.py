from web3 import Web3, HTTPProvider,WebsocketProvider
from hexbytes import (
    HexBytes,
)
from collections import defaultdict
from loguru import logger
import redis
import time
import sys
import os

r = redis.Redis(host="localhost", port=6379, db=0)
rpc = "https://rpc1.inspace.network/"
w3 = Web3(HTTPProvider(rpc))
logger.add("scan_ins.log", rotation="10 MB")


class Scan:
    def __init__(self, name: str, start_block: int, mint_hash: str) -> None:
        self.name = name
        if r.get(self.name):
            self.last_run_block = r.get(self.name)
        else:
            self.last_run_block = start_block
            r.set(name, start_block)

            total_mint_key = f"{self.name}_total"
            r.set(total_mint_key, 0)
        self.mint_hash = HexBytes(mint_hash)
        logger.info(f"minthash: {self.mint_hash}")
        logger.info(f"start {int(self.last_run_block)}")

    def get_block_transactions(self, block_num):
        block_txs = w3.eth.get_block(block_num, full_transactions=True)
        transactions = block_txs["transactions"]
        return transactions

    def get_transactions(self, block_num):
        transactions = self.get_block_transactions(block_num)
        transactions = [i for i in transactions if i["input"] == self.mint_hash]
        transactions = [i for i in transactions if i["from"] == i["to"]]
        return transactions

    def handle_transactions(self, block_number):
        mint_info = defaultdict(int)
        transactions = self.get_transactions(block_number)
        for transaction in transactions:
            mint_owner = transaction["from"]
            mint_info[mint_owner] += 1000
        return mint_info

    def add_to_db(self, mint_info, block_num):
        try:
            pipe = r.pipeline()

            pipe.set(self.name, block_num)
            self.last_run_block = block_num
            logger.info(f"block:{block_num}")

            total_mint_key = f"{self.name}_total"

            for mint_owner, mint_count in mint_info.items():
                key_name = f"{self.name}_{mint_owner}"
                # logger.debug(f"{key_name} mint了 {mint_count}张")
                pipe.incrby(key_name, mint_count)
                pipe.incrby(total_mint_key, mint_count)

            result = pipe.execute()



        except Exception as e:
            logger.exception("Redis error")

    def run(self):
        last_block_number = w3.eth.block_number
        for block_num in range(int(self.last_run_block), last_block_number):
            mint_info = self.handle_transactions(block_num)
            self.add_to_db(mint_info, block_num)

    def run_forever(self):
        while True:
            try:
                logger.info('start scan block.')
                self.run()
            except Exception as e:
                logger.error(f"error：{e}")
                time.sleep(5)
                os.execv(sys.executable, ['python'] + sys.argv)


if __name__ == "__main__":
    data = 'data:,{"p":"ins-20","op":"mint","tick":"ispa","amt":"1000"}'
    mint_hash = w3.to_hex(text=data)
    tickname = "ispa_prod"
    start_block = 1185
    eths = Scan(tickname, start_block, mint_hash)
    eths.run_forever()
