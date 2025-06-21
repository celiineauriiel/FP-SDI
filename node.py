# eas/node.py
from storage.hot_storage import HotStorage
from storage.cold_storage import ColdStorage

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.hot_storage = HotStorage()
        self.cold_storage = ColdStorage(directory=f"cold_storage_{self.node_id}")

    def write_to_hot(self, key, value_bytes):
        print(f"  NODE {self.node_id}: Menerima & menyimpan key='{key}' di Hot Storage.")
        self.hot_storage.put(key, value_bytes)
        return True

    def migrate_to_cold(self, key):
        value_bytes = self.hot_storage.get(key)
        if value_bytes is not None:
            self.cold_storage.put(key, value_bytes)
            self.hot_storage.remove(key)
            return True
        return False