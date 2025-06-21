# eas/consistent_hash.py
import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, nodes=None, num_replicas=3, num_partitions=10):
        self.num_replicas = num_replicas
        self.num_partitions = num_partitions
        self.ring = dict()
        self._sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.num_replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            self._sorted_keys.append(key)
        self._sorted_keys.sort()

    def get_nodes(self, key, count):
        if not self.ring: return []
        count = min(count, len(set(self.ring.values())))
        found_nodes = []
        key_hash = self._hash(key)
        start_idx = bisect.bisect_left(self._sorted_keys, key_hash)
        idx = start_idx
        while len(found_nodes) < count:
            if idx == len(self._sorted_keys):
                idx = 0
            node_key = self._sorted_keys[idx]
            node_name = self.ring[node_key]
            if node_name not in found_nodes:
                found_nodes.append(node_name)
            idx += 1
        return found_nodes

    def get_partition(self, key):
        return self._hash(key) % self.num_partitions