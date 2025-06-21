# eas/storage/cold_storage.py
import os
import gzip

class ColdStorage:
    def __init__(self, directory):
        self.directory = directory
        if not os.path.exists(directory):
            os.makedirs(directory)
            
    def put(self, key, value_bytes):
        filename = os.path.join(self.directory, f"{key}.bin.gz")
        with gzip.open(filename, "wb") as f:
            f.write(value_bytes)
            
    def get(self, key):
        filename = os.path.join(self.directory, f"{key}.bin.gz")
        if os.path.exists(filename):
            with gzip.open(filename, "rb") as f:
                return f.read()
        return None
        
    def list_keys(self):
        """Metode baru untuk mendapatkan semua kunci di cold storage."""
        keys = []
        for filename in os.listdir(self.directory):
            if filename.endswith(".bin.gz"):
                keys.append(filename[:-7]) # Hapus ekstensi .bin.gz
        return keys