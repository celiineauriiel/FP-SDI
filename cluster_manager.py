# eas/cluster_manager.py
from consistent_hash import ConsistentHashRing

class ClusterManager:
    def __init__(self, replication_factor=3, num_partitions=10):
        self.nodes = {}
        self.replication_factor = replication_factor
        self.ring = ConsistentHashRing(num_partitions=num_partitions)

    def add_node(self, node):
        self.nodes[node.node_id] = node
        self.ring.add_node(node.node_id)

    def remove_node(self, node_id):
        """Metode baru untuk mensimulasikan node failure."""
        if node_id in self.nodes:
            print(f"!!! SIMULASI GAGAL: Node {node_id} dimatikan.")
            del self.nodes[node_id]
            # Update ring consistent hashing tanpa node yang gagal
            self.ring = ConsistentHashRing(nodes=list(self.nodes.keys()))
            return True
        return False

    def get_responsible_nodes(self, key):
        node_ids = self.ring.get_nodes(key, self.replication_factor)
        return [self.nodes[node_id] for node_id in node_ids]

    def put(self, key, value_bytes):
        nodes = self.get_responsible_nodes(key)
        ack_count = 0
        for node in nodes:
            if node.write_to_hot(key, value_bytes):
                ack_count += 1
        return ack_count, len(nodes)

    def get(self, key):
        nodes = self.get_responsible_nodes(key)
        if not nodes: return None, None
        primary_node = nodes[0]
        value_bytes, location = primary_node.hot_storage.get(key), "HOT"
        if value_bytes is None:
            value_bytes, location = primary_node.cold_storage.get(key), "COLD"
        return value_bytes, location

    def migrate(self, key):
        nodes = self.get_responsible_nodes(key)
        success_count = 0
        for node in nodes:
            if node.migrate_to_cold(key):
                success_count += 1
        return success_count > 0