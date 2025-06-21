# eas/cli.py (Versi Final Modular)
import os
import shutil
import json
import sys

# Impor kelas-kelas dari file lain
from node import Node
from cluster_manager import ClusterManager
from data_v1_pb2 import KeyValue as KeyValue_v1
from data_v2_pb2 import KeyValue as KeyValue_v2
from data_v3_pb2 import KeyValue as KeyValue_v3
from data_v4_pb2 import KeyValue as KeyValue_v4

# --- BAGIAN UTAMA: CLI & FUNGSI HELPER ---
def describe_object(version_to_parse_as, obj_to_describe):
    description = {"key": obj_to_describe.key}
    if version_to_parse_as == 'v1': description["content"] = obj_to_describe.content
    elif version_to_parse_as == 'v2':
        description["content"] = obj_to_describe.content
        description["author"] = obj_to_describe.author
    elif version_to_parse_as == 'v3':
        description["author"] = obj_to_describe.author
        description["value"] = obj_to_describe.value
    elif version_to_parse_as == 'v4':
        description["author"] = obj_to_describe.author
        description["value"] = obj_to_describe.value
        description["status"] = KeyValue_v4.Status.Name(obj_to_describe.status)
    if hasattr(obj_to_describe, 'version') and obj_to_describe.version > 0:
        description["version_in_data"] = obj_to_describe.version
    return description

def start_cli():
    # ... (Seluruh loop `while True` dan logikanya sama persis seperti jawaban terakhir)
    if '--clean' in sys.argv:
        print("Membersihkan Cold Storage lama...")
        for dir_name in ["cold_storage_node-1", "cold_storage_node-2", "cold_storage_node-3"]:
            if os.path.exists(dir_name): shutil.rmtree(dir_name)
    
    num_partitions, write_quorum, replication_factor = 10, 2, 3
    cluster = ClusterManager(replication_factor=replication_factor, num_partitions=num_partitions)
    cluster.add_node(Node(node_id="node-1")); cluster.add_node(Node(node_id="node-2")); cluster.add_node(Node(node_id="node-3"))
    print(f"Konfigurasi Kuorum: n={replication_factor}, w={write_quorum}"); print("--- Peta Partisi (Replica Set) ---")
    for i in range(num_partitions):
        nodes = cluster.ring.get_nodes(f"partition-{i}", replication_factor); print(f"Partisi {i}: {nodes}")
    print("------------------------------------"); print("Ketik 'help' untuk bantuan.")
    schema_map = {'v1': KeyValue_v1, 'v2': KeyValue_v2, 'v3': KeyValue_v3, 'v4': KeyValue_v4}
    
    while True:
        try:
            line = input("(kv-db) > ")
            if not line: continue
            parts = line.split(); command, args = parts[0].lower(), parts[1:]

            if command == "put":
                if len(args) < 3: print("Error: Gunakan 'put <kunci> <versi> [args...]'"); continue
                key, version, put_args = args[0], args[1], args[2:]
                if version not in schema_map: print(f"Error: Versi '{version}' tidak dikenal."); continue
                partition_id = cluster.ring.get_partition(key)
                node_names = [n.node_id for n in cluster.get_responsible_nodes(key)]
                print(f"INFO: Key='{key}' -> Partisi={partition_id} -> Direplikasi ke: {node_names}")
                try:
                    obj = None
                    if version == 'v1': obj = schema_map[version](key=key, content=" ".join(put_args), version=1)
                    elif version == 'v2': obj = schema_map[version](key=key, content=put_args[0], author=put_args[1], version=2)
                    elif version == 'v3': obj = schema_map[version](key=key, author=put_args[0], value=int(put_args[1]), version=3)
                    elif version == 'v4': obj = schema_map[version](key=key, author=put_args[0], value=int(put_args[1]), status=KeyValue_v4.Status.Value(put_args[2].upper()), version=4)
                    ack_count, total_nodes = cluster.put(key, obj.SerializeToString())
                    status = "terpenuhi" if ack_count >= write_quorum else "TIDAK terpenuhi"
                    print(f"CLIENT: PUT berhasil, {ack_count}/{total_nodes} node merespons (kuorum w={write_quorum} {status}).")
                except Exception as e: print(f"Error: Argumen untuk 'put {version}' tidak sesuai. {e}")
            elif command == "get":
                if len(args) != 2: print("Error: Gunakan 'get <versi> <kunci>'"); continue
                version, key = args[0], args[1]
                if version not in schema_map: print(f"Error: Versi '{version}' tidak dikenal."); continue
                data_bytes, location = cluster.get(key)
                if data_bytes:
                    parser_obj = schema_map[version](); parser_obj.ParseFromString(data_bytes)
                    description = describe_object(version, parser_obj)
                    print(f"-> Lokasi: {location}, Dibaca sebagai {version.upper()}"); print(json.dumps(description, indent=2))
                else: print("(nil)")
            elif command == "inspect":
                 if len(args) != 1: print("Error: Gunakan 'inspect <kunci>'"); continue
                 key = args[0]; data_bytes, location = cluster.get(key)
                 print(f"\n--- HASIL INSPEKSI UNTUK KUNCI: '{key}' ---")
                 if data_bytes:
                     v_check = KeyValue_v4(); v_check.ParseFromString(data_bytes)
                     actual_version = f"v{v_check.version}" if hasattr(v_check, 'version') and v_check.version > 0 else "v?"
                     if actual_version in schema_map:
                         parser_obj = schema_map[actual_version](); parser_obj.ParseFromString(data_bytes)
                         description = describe_object(actual_version, parser_obj)
                         print(f"Ditemukan di: {location} Storage"); print("Bentuk Skema & Isi Data:"); print(json.dumps(description, indent=2))
                     else: print(f"Ditemukan di: {location} Storage, tapi versi tidak dikenal.")
                 else: print("Data tidak ditemukan di Hot maupun Cold storage.")
                 print("------------------------------------------\n")
            elif command == "inspect-node":
                if len(args) != 1: print("Error: Gunakan 'inspect-node <node_id>'"); continue
                node_id = args[0]
                if node_id not in cluster.nodes: print(f"Error: Node '{node_id}' tidak ditemukan."); continue
                node = cluster.nodes[node_id]
                print(f"\n--- INSPEKSI LENGKAP UNTUK NODE: {node_id} ---")
                def inspect_storage(storage_name, getter_func_keys, getter_func_bytes):
                    print(f"--- {storage_name} ---")
                    keys = getter_func_keys()
                    if not keys: print("  (Kosong)")
                    else:
                        for key in keys:
                            data_bytes = getter_func_bytes(key)
                            if not data_bytes: continue
                            v_check = KeyValue_v4(); v_check.ParseFromString(data_bytes)
                            actual_version = f"v{v_check.version}"
                            if actual_version in schema_map:
                                parser_obj = schema_map[actual_version](); parser_obj.ParseFromString(data_bytes)
                                description = describe_object(actual_version, parser_obj)
                                print(f"  Kunci: {key}"); print(f"    {json.dumps(description)}")
                            else: print(f"  Kunci: {key} (versi tidak dikenal)")
                inspect_storage("Hot Storage (Memori)", lambda: node.hot_storage.cache.keys(), node.hot_storage.get)
                print()
                if hasattr(node.cold_storage, 'list_keys'):
                     inspect_storage("Cold Storage (Disk)", node.cold_storage.list_keys, node.cold_storage.get)
                else: print("--- Cold Storage (Disk) ---\n  (Fungsi list_keys tidak ditemukan di ColdStorage)")
                print("----------------------------------------\n")
            elif command == "migrate":
                if len(args) != 1: print("Error: Gunakan 'migrate <kunci>'"); continue
                if cluster.migrate(args[0]): print(f"Kunci '{args[0]}' berhasil dimigrasikan.")
                else: print(f"Error: Kunci '{args[0]}' tidak ditemukan di Hot Storage.")
            elif command == 'help':
                print("Perintah yang tersedia:\n  put <kunci> <versi> [args...]\n  get <versi> <kunci>\n  inspect <kunci>\n  inspect-node <node_id>\n  migrate <kunci>\n  exit / quit / q")
            elif command in ["exit", "quit", "q"]: print("Keluar."); break
            else: print(f"Error: Perintah tidak dikenal '{command}'.")
        except KeyboardInterrupt: print("\nKeluar."); break
        except Exception as e: print(f"Terjadi error tak terduga: {e}")

if __name__ == "__main__":
    start_cli()