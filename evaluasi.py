# eas/evaluasi.py (Diperbaiki untuk Struktur Modular)
import time
import random
import string
import os
import matplotlib.pyplot as plt
import shutil

# --- PERBAIKAN IMPORT DI SINI ---
# Impor kelas-kelas dari filenya masing-masing, bukan dari cli.py
from cluster_manager import ClusterManager
from node import Node
from data_v4_pb2 import KeyValue as KeyValue_v4 
# --------------------------------

# --- FUNGSI-FUNGSI SETUP ---

def setup_cluster():
    """Membuat instance cluster yang bersih untuk setiap tes."""
    for dir_name in ["cold_storage_node-1", "cold_storage_node-2", "cold_storage_node-3"]:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
    
    cluster = ClusterManager(replication_factor=3, num_partitions=10)
    cluster.add_node(Node(node_id="node-1"))
    cluster.add_node(Node(node_id="node-2"))
    cluster.add_node(Node(node_id="node-3"))
    return cluster

def generate_random_key(length=8):
    """Membuat kunci acak untuk pengujian."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

# --- FUNGSI-FUNGSI PENGUJIAN ---

def test_throughput_latency(num_operations=100):
    """Menguji Throughput dan Latency untuk operasi Read dan Write."""
    print("\n" + "="*50)
    print("1. MENGUJI THROUGHPUT & LATENCY")
    print("="*50)
    
    cluster = setup_cluster()
    keys = [generate_random_key() for _ in range(num_operations)]
    value_obj = KeyValue_v4(author="tester", value=123, status=KeyValue_v4.ACTIVE, version=4)
    value_bytes = value_obj.SerializeToString()

    # --- Uji Write ---
    start_time = time.time()
    for key in keys:
        cluster.put(key, value_bytes)
    end_time = time.time()
    
    write_duration = end_time - start_time
    write_throughput = num_operations / write_duration
    write_latency = write_duration / num_operations
    
    print(f"[WRITE] Selesai {num_operations} operasi dalam {write_duration:.4f} detik.")
    print(f"  -> Write Throughput: {write_throughput:.2f} operasi/detik")
    print(f"  -> Rata-rata Write Latency: {write_latency * 1000:.4f} ms/operasi")

    # --- Uji Read ---
    start_time = time.time()
    for key in keys:
        cluster.get(key)
    end_time = time.time()

    read_duration = end_time - start_time
    read_throughput = num_operations / read_duration
    read_latency = read_duration / num_operations

    print(f"\n[READ] Selesai {num_operations} operasi dalam {read_duration:.4f} detik.")
    print(f"  -> Read Throughput: {read_throughput:.2f} operasi/detik")
    print(f"  -> Rata-rata Read Latency: {read_latency * 1000:.4f} ms/operasi")


def test_fault_tolerance():
    """Mensimulasikan kegagalan node dan memeriksa apakah data tetap bisa diakses."""
    print("\n" + "="*50)
    print("2. MENGUJI FAULT TOLERANCE (NODE FAILURE)")
    print("="*50)

    cluster = setup_cluster()
    test_key = "kunci_penting"
    test_value_obj = KeyValue_v4(key=test_key, author="celine", value=100, version=4)
    test_value_bytes = test_value_obj.SerializeToString()

    # 1. Simpan data dalam kondisi normal
    print(f"[TAHAP 1] Menyimpan '{test_key}' ke cluster...")
    cluster.put(test_key, test_value_bytes)
    
    # 2. Identifikasi dan "matikan" node primer
    responsible_nodes = cluster.get_responsible_nodes(test_key)
    primary_node_id = responsible_nodes[0].node_id
    print(f"Node primer untuk '{test_key}' adalah {primary_node_id}.")
    
    # Tambahkan metode remove_node ke ClusterManager jika belum ada
    if not hasattr(cluster, 'remove_node'):
        def remove_node(self, node_id):
            if node_id in self.nodes:
                print(f"!!! SIMULASI GAGAL: Node {node_id} dimatikan.")
                del self.nodes[node_id]
                self.ring = type(self.ring)(nodes=list(self.nodes.keys())) # Re-create ring
                return True
            return False
        import types
        cluster.remove_node = types.MethodType(remove_node, cluster)
        
    cluster.remove_node(primary_node_id)
    
    # 3. Coba baca kembali data yang sama
    print(f"\n[TAHAP 2] Mencoba mengambil '{test_key}' setelah node primer gagal...")
    retrieved_bytes, location = cluster.get(test_key)
    
    if retrieved_bytes:
        retrieved_obj = KeyValue_v4()
        retrieved_obj.ParseFromString(retrieved_bytes)
        if retrieved_obj.value == test_value_obj.value:
            print(f"-> SUKSES! Data berhasil diambil dari replika (Lokasi: {location} Storage).")
            print("-> Sistem menunjukkan Fault Tolerance yang baik.")
        else:
            print("-> GAGAL! Data yang diambil korup.")
    else:
        print("-> GAGAL! Data tidak dapat ditemukan setelah node primer mati.")


def test_hot_cold_performance(num_operations=50):
    """Membandingkan performa baca dari Hot Storage (RAM) vs Cold Storage (Disk)."""
    print("\n" + "="*50)
    print("3. GRAFIK PERBANDINGAN HOT VS COLD STORAGE")
    print("="*50)

    cluster = setup_cluster()
    keys = [generate_random_key() for _ in range(num_operations)]
    value_obj = KeyValue_v4(author="tester", value=123, status=KeyValue_v4.ACTIVE, version=4)
    value_bytes = value_obj.SerializeToString()

    print(f"[TAHAP 1] Mengisi {num_operations} data ke Hot Storage...")
    for key in keys:
        cluster.put(key, value_bytes)
    
    # --- Ukur Performa Baca Hot Storage ---
    start_time_hot = time.time()
    for key in keys:
        cluster.get(key)
    end_time_hot = time.time()
    hot_duration = end_time_hot - start_time_hot
    print(f"-> Waktu baca dari Hot Storage (RAM): {hot_duration:.4f} detik.")

    # --- Migrasi semua data ke Cold Storage ---
    print(f"\n[TAHAP 2] Memigrasikan {num_operations} data ke Cold Storage...")
    for key in keys:
        cluster.migrate(key)
    print("-> Migrasi selesai.")

    # --- Ukur Performa Baca Cold Storage ---
    start_time_cold = time.time()
    for key in keys:
        cluster.get(key)
    end_time_cold = time.time()
    cold_duration = end_time_cold - start_time_cold
    print(f"-> Waktu baca dari Cold Storage (Disk): {cold_duration:.4f} detik.")

    # --- Buat dan simpan grafik ---
    print("\n[TAHAP 3] Membuat grafik perbandingan...")
    labels = ['Hot Storage (RAM)', 'Cold Storage (Disk)']
    times = [hot_duration, cold_duration]
    
    plt.figure(figsize=(8, 6))
    bars = plt.bar(labels, times, color=['#3498db', '#e74c3c'])
    plt.ylabel('Waktu Eksekusi (detik)')
    plt.title(f'Perbandingan Performa Baca Hot vs Cold Storage ({num_operations} operasi)')
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:.4f}s', va='bottom')

    plt.savefig('hot_vs_cold_performance.png')
    print("-> SUKSES! Grafik telah disimpan sebagai 'hot_vs_cold_performance.png'")


# --- EKSEKUSI UTAMA ---
if __name__ == "__main__":
    print("===== MEMULAI UJI COBA PERFORMA SISTEM =====")
    test_throughput_latency()
    test_fault_tolerance()
    test_hot_cold_performance()
    print("\n===== UJI COBA SELESAI =====")