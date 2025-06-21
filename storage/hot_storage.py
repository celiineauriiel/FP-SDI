# eas/storage/hot_storage.py
class HotStorage:
    def __init__(self):
        self.cache = {}
    def put(self, key, value_bytes):
        self.cache[key] = value_bytes
    def get(self, key):
        return self.cache.get(key)
    def remove(self, key):
        if key in self.cache:
            del self.cache[key]