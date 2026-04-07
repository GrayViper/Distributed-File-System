import threading
import time
import random
import hashlib
import json
from collections import defaultdict
from typing import Dict, List, Set, Optional

# -------------------------------
# GLOBAL CONFIGURATION
# -------------------------------
REPLICATION_FACTOR = 3
CHUNK_SIZE = 1024
HEARTBEAT_INTERVAL = 2
NODE_TIMEOUT = 10
MAX_RETRIES = 3

# -------------------------------
# THREAD-SAFE PRINTING
# -------------------------------
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs, flush=True)

# -------------------------------
# DATA STRUCTURES
# -------------------------------
class ChunkMetadata:
    def __init__(self, chunk_id, file_name, chunk_index, size, checksum):
        self.chunk_id = chunk_id
        self.file_name = file_name
        self.chunk_index = chunk_index
        self.size = size
        self.checksum = checksum
        self.replicas: Set[str] = set()

class FileMetadata:
    def __init__(self, file_name, total_size, total_chunks):
        self.file_name = file_name
        self.total_size = total_size
        self.total_chunks = total_chunks
        self.chunks: List[str] = []

# -------------------------------
# MASTER NODE
# -------------------------------
class MasterNode:
    def __init__(self):
        self.nodes = {}
        self.node_objects = {}
        self.files = {}
        self.chunks = {}
        self.lock = threading.Lock()
        self.running = True

        threading.Thread(target=self._monitor_nodes, daemon=True).start()

    def register_node(self, node_id, node_info, node_obj):
        with self.lock:
            self.nodes[node_id] = {
                **node_info,
                'last_heartbeat': time.time(),
                'status': 'alive'
            }
            self.node_objects[node_id] = node_obj
            safe_print(f"📝 Node {node_id} registered")

    def heartbeat(self, node_id, node_info):
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].update({
                    **node_info,
                    'last_heartbeat': time.time(),
                    'status': 'alive'
                })

    def get_alive_nodes(self):
        now = time.time()
        return [n for n, info in self.nodes.items()
                if now - info['last_heartbeat'] < NODE_TIMEOUT]

    def get_node_score(self, node_info):
        return (node_info['free_space'] * 0.6
                - node_info['cpu_usage'] * 0.2
                - node_info['active_requests'] * 1.0)

    def select_replica_nodes(self):
        nodes = self.get_alive_nodes()
        if len(nodes) < REPLICATION_FACTOR:
            safe_print("⚠️ Reduced replication")
            return nodes

        scored = sorted(nodes,
                        key=lambda n: self.get_node_score(self.nodes[n]),
                        reverse=True)
        return scored[:REPLICATION_FACTOR]

    def store_file(self, file_name, data):
        chunks = [data[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
        file_meta = FileMetadata(file_name, len(data), len(chunks))
        self.files[file_name] = file_meta

        for i, chunk in enumerate(chunks):
            chunk_id = f"{file_name}_{i}"
            checksum = hashlib.md5(chunk).hexdigest()
            meta = ChunkMetadata(chunk_id, file_name, i, len(chunk), checksum)
            self.chunks[chunk_id] = meta
            file_meta.chunks.append(chunk_id)

            nodes = self.select_replica_nodes()

            for node_id in nodes:
                node = self.node_objects[node_id]

                for _ in range(MAX_RETRIES):
                    if node.active_requests > 5:
                        time.sleep(0.05)
                        continue

                    if node.store_chunk(chunk_id, chunk):
                        meta.replicas.add(node_id)
                        break

        safe_print(f"✅ Stored {file_name}")
        return True

    def retrieve_file(self, file_name):
        if file_name not in self.files:
            return None

        data_parts = []
        for chunk_id in self.files[file_name].chunks:
            chunk = None

            for _ in range(MAX_RETRIES):
                chunk = self._retrieve_chunk(chunk_id)
                if chunk:
                    break
                time.sleep(0.05)

            if not chunk:
                return None

            data_parts.append(chunk)

        return b''.join(data_parts)

    def _retrieve_chunk(self, chunk_id):
        meta = self.chunks[chunk_id]
        nodes = list(meta.replicas)

        nodes.sort(key=lambda n: self.nodes[n]['active_requests'])

        for node_id in nodes:
            node = self.node_objects[node_id]
            data = node.retrieve_chunk(chunk_id)
            if data:
                return data

        return None

    def _monitor_nodes(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            now = time.time()

            for node_id, info in self.nodes.items():
                if now - info['last_heartbeat'] > NODE_TIMEOUT:
                    if info['status'] == 'alive':
                        info['status'] = 'dead'
                        safe_print(f"💀 Node {node_id} dead")

# -------------------------------
# DATA NODE
# -------------------------------
class DataNode:
    def __init__(self, node_id, master):
        self.node_id = node_id
        self.master = master
        self.storage = {}
        self.active_requests = 0
        self.running = True

        self.cpu_usage = random.randint(10, 90)
        self.free_space = random.randint(500, 2000)

        threading.Thread(target=self._heartbeat, daemon=True).start()

    def _heartbeat(self):
        while self.running:
            self.master.heartbeat(self.node_id, {
                'cpu_usage': self.cpu_usage,
                'free_space': self.free_space,
                'active_requests': self.active_requests
            })
            time.sleep(HEARTBEAT_INTERVAL)

    def store_chunk(self, chunk_id, data):
        self.active_requests += 1

        if self.active_requests > 8:
            time.sleep(0.1)

        time.sleep(random.uniform(0.01, 0.05))
        self.storage[chunk_id] = data

        self.active_requests -= 1
        safe_print(f"💾 {self.node_id} stored {chunk_id}")
        return True

    def retrieve_chunk(self, chunk_id):
        self.active_requests += 1
        time.sleep(random.uniform(0.01, 0.03))

        data = self.storage.get(chunk_id)

        self.active_requests -= 1
        return data

# -------------------------------
# CLIENT
# -------------------------------
class DFSClient:
    def __init__(self, master):
        self.master = master

    def upload_file(self, name, data):
        safe_print(f"📤 Upload {name}")
        return self.master.store_file(name, data)

    def download_file(self, name):
        safe_print(f"📥 Download {name}")
        return self.master.retrieve_file(name)

# -------------------------------
# SIMULATION
# -------------------------------
def simulate():
    master = MasterNode()

    nodes = {}
    for i in range(5):
        node = DataNode(f"node_{i}", master)
        nodes[node.node_id] = node

        master.register_node(node.node_id, {
            'cpu_usage': node.cpu_usage,
            'free_space': node.free_space,
            'active_requests': 0
        }, node)

    client = DFSClient(master)

    time.sleep(2)

    files = {
        "file1.txt": b"A" * 3000,
        "file2.txt": b"B" * 2000
    }

    for name, data in files.items():
        client.upload_file(name, data)

    for name in files:
        result = client.download_file(name)
        if result == files[name]:
            safe_print(f"✅ {name} OK")
        else:
            safe_print(f"❌ {name} FAILED")

if __name__ == "__main__":
    simulate()
