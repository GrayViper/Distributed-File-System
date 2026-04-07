import threading
import time
import random
import hashlib
import os
from collections import defaultdict
from typing import Dict, List, Set, Optional

# -------------------------------
# CONFIGURATION
# -------------------------------
REPLICATION_FACTOR = 3
CHUNK_SIZE = 1024
HEARTBEAT_INTERVAL = 2
NODE_TIMEOUT = 10
MAX_RETRIES = 3
MIN_NODES = 5  # Minimum number of nodes to maintain
MAX_NODES = 10  # Maximum number of nodes

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
    def __init__(self, chunk_id: str, file_name: str, chunk_index: int, size: int, checksum: str):
        self.chunk_id = chunk_id
        self.file_name = file_name
        self.chunk_index = chunk_index
        self.size = size
        self.checksum = checksum
        self.replicas: Set[str] = set()

class FileMetadata:
    def __init__(self, file_name: str, total_size: int, total_chunks: int):
        self.file_name = file_name
        self.total_size = total_size
        self.total_chunks = total_chunks
        self.chunks: List[str] = []

# -------------------------------
# DATA NODE
# -------------------------------
class DataNode:
    def __init__(self, node_id: str, master):
        self.node_id = node_id
        self.master = master
        self.storage: Dict[str, bytes] = {}
        self.active_requests = 0
        self.running = True
        self.cpu_usage = random.randint(10, 90)
        self.free_space = random.randint(500, 2000)

        threading.Thread(target=self._heartbeat, daemon=True).start()
        safe_print(f"🆕 Node {node_id} created")

    def _heartbeat(self):
        while self.running:
            self.master.heartbeat(self.node_id, {
                'cpu_usage': self.cpu_usage,
                'free_space': self.free_space,
                'active_requests': self.active_requests
            })
            time.sleep(HEARTBEAT_INTERVAL)

    def store_chunk(self, chunk_id: str, data: bytes) -> bool:
        self.active_requests += 1

        if self.active_requests > 8:
            time.sleep(0.1)

        time.sleep(random.uniform(0.01, 0.05))
        self.storage[chunk_id] = data

        self.active_requests -= 1
        safe_print(f"💾 {self.node_id} stored {chunk_id}")
        return True

    def retrieve_chunk(self, chunk_id: str) -> Optional[bytes]:
        self.active_requests += 1
        time.sleep(random.uniform(0.01, 0.03))

        data = self.storage.get(chunk_id)

        self.active_requests -= 1
        return data

    def shutdown(self):
        self.running = False
        safe_print(f"🛑 Node {self.node_id} shutting down")

# -------------------------------
# MASTER NODE
# -------------------------------
class MasterNode:
    def __init__(self):
        self.nodes: Dict[str, dict] = {}
        self.node_objects: Dict[str, DataNode] = {}
        self.files: Dict[str, FileMetadata] = {}
        self.chunks: Dict[str, ChunkMetadata] = {}
        self.lock = threading.Lock()
        self.running = True
        self.node_counter = 0

        # Start monitoring threads
        threading.Thread(target=self._monitor_nodes, daemon=True).start()
        threading.Thread(target=self._self_heal, daemon=True).start()
        threading.Thread(target=self._node_generation, daemon=True).start()

    def create_node(self) -> str:
        with self.lock:
            node_id = f"node_{self.node_counter}"
            self.node_counter += 1
            node = DataNode(node_id, self)
            self.node_objects[node_id] = node

            self.nodes[node_id] = {
                'cpu_usage': node.cpu_usage,
                'free_space': node.free_space,
                'active_requests': 0,
                'last_heartbeat': time.time(),
                'status': 'alive'
            }
            return node_id

    def register_node(self, node_id: str, node_info: dict, node_obj: DataNode):
        with self.lock:
            self.nodes[node_id] = {
                **node_info,
                'last_heartbeat': time.time(),
                'status': 'alive'
            }
            self.node_objects[node_id] = node_obj
            safe_print(f"📝 Node {node_id} registered")

    def heartbeat(self, node_id: str, node_info: dict):
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].update({
                    **node_info,
                    'last_heartbeat': time.time(),
                    'status': 'alive'
                })

    def get_alive_nodes(self) -> List[str]:
        now = time.time()
        return [n for n, info in self.nodes.items()
                if now - info['last_heartbeat'] < NODE_TIMEOUT]

    def get_node_score(self, node_info: dict) -> float:
        return (node_info['free_space'] * 0.6
                - node_info['cpu_usage'] * 0.2
                - node_info['active_requests'] * 1.0)

    def select_replica_nodes(self) -> List[str]:
        nodes = self.get_alive_nodes()
        if len(nodes) < REPLICATION_FACTOR:
            safe_print("⚠️ Reduced replication")
            return nodes

        scored = sorted(nodes,
                        key=lambda n: self.get_node_score(self.nodes[n]),
                        reverse=True)
        return scored[:REPLICATION_FACTOR]

    def store_file(self, file_name: str, data: bytes) -> bool:
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

    def retrieve_file(self, file_name: str) -> Optional[bytes]:
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

    def _retrieve_chunk(self, chunk_id: str) -> Optional[bytes]:
        meta = self.chunks[chunk_id]
        nodes = list(meta.replicas)

        nodes.sort(key=lambda n: self.nodes[n]['active_requests'])

        for node_id in nodes:
            if node_id in self.node_objects:
                node = self.node_objects[node_id]
                data = node.retrieve_chunk(chunk_id)
                if data:
                    return data

        return None

    def _monitor_nodes(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            now = time.time()

            with self.lock:
                for node_id, info in self.nodes.items():
                    if now - info['last_heartbeat'] > NODE_TIMEOUT:
                        if info['status'] == 'alive':
                            info['status'] = 'dead'
                            safe_print(f"💀 Node {node_id} detected dead")
                            # Trigger self-healing
                            threading.Thread(target=self._heal_node_failure, args=(node_id,), daemon=True).start()

    def _heal_node_failure(self, dead_node_id: str):
        safe_print(f"🔄 Starting self-healing for dead node {dead_node_id}")

        # Re-replicate chunks that were on the dead node
        affected_chunks = []
        for chunk_id, meta in self.chunks.items():
            if dead_node_id in meta.replicas:
                meta.replicas.discard(dead_node_id)
                if len(meta.replicas) < REPLICATION_FACTOR:
                    affected_chunks.append(chunk_id)

        for chunk_id in affected_chunks:
            self._re_replicate_chunk(chunk_id)

    def _re_replicate_chunk(self, chunk_id: str):
        meta = self.chunks[chunk_id]
        current_replicas = list(meta.replicas)

        if not current_replicas:
            safe_print(f"❌ No replicas left for {chunk_id} - data loss!")
            return

        # Get data from existing replica
        source_node_id = current_replicas[0]
        source_node = self.node_objects[source_node_id]
        data = source_node.retrieve_chunk(chunk_id)

        if not data:
            safe_print(f"❌ Could not retrieve {chunk_id} from {source_node_id}")
            return

        # Select new nodes for replication
        available_nodes = [n for n in self.get_alive_nodes() if n not in current_replicas]
        needed = REPLICATION_FACTOR - len(current_replicas)

        if len(available_nodes) < needed:
            safe_print(f"⚠️ Not enough nodes to fully re-replicate {chunk_id}")
            needed = len(available_nodes)

        # Score and select best nodes
        scored_nodes = sorted(available_nodes,
                             key=lambda n: self.get_node_score(self.nodes[n]),
                             reverse=True)[:needed]

        for node_id in scored_nodes:
            node = self.node_objects[node_id]
            if node.store_chunk(chunk_id, data):
                meta.replicas.add(node_id)
                safe_print(f"♻️ Re-replicated {chunk_id} to {node_id}")

    def _self_heal(self):
        while self.running:
            time.sleep(5)  # Check every 5 seconds

            # Check for under-replicated chunks
            for chunk_id, meta in list(self.chunks.items()):
                if len(meta.replicas) < REPLICATION_FACTOR:
                    alive_replicas = [n for n in meta.replicas if n in self.get_alive_nodes()]
                    if len(alive_replicas) < REPLICATION_FACTOR:
                        safe_print(f"🔄 Self-healing: re-replicating {chunk_id}")
                        self._re_replicate_chunk(chunk_id)

    def _node_generation(self):
        while self.running:
            time.sleep(10)  # Check every 10 seconds

            alive_count = len(self.get_alive_nodes())

            if alive_count < MIN_NODES:
                needed = MIN_NODES - alive_count
                safe_print(f"🏭 Generating {needed} new nodes (alive: {alive_count})")

                for _ in range(needed):
                    if len(self.nodes) < MAX_NODES:
                        self.create_node()
                    else:
                        safe_print("⚠️ Reached maximum node limit")
                        break

    def shutdown(self):
        self.running = False
        for node in self.node_objects.values():
            node.shutdown()

# -------------------------------
# CLIENT
# -------------------------------
class DFSClient:
    def __init__(self, master: MasterNode):
        self.master = master

    def upload_file(self, name: str, data: bytes) -> bool:
        safe_print(f"📤 Uploading {name}")
        return self.master.store_file(name, data)

    def download_file(self, name: str) -> Optional[bytes]:
        safe_print(f"📥 Downloading {name}")
        return self.master.retrieve_file(name)

    def list_files(self) -> List[str]:
        return list(self.master.files.keys())

# -------------------------------
# SIMULATION
# -------------------------------
def simulate():
    master = MasterNode()

    # Create initial nodes
    for i in range(5):
        master.create_node()

    client = DFSClient(master)

    time.sleep(3)

    # Upload some files
    files = {
        "file1.txt": b"Hello, this is a distributed file system with fault tolerance!" * 10,
        "file2.txt": b"This system has self-healing capabilities." * 8,
        "file3.txt": b"Node generation ensures cluster health." * 6
    }

    for name, data in files.items():
        client.upload_file(name, data)

    time.sleep(2)

    # Simulate node failures
    safe_print("\n💥 Simulating node failures...")
    alive_nodes = master.get_alive_nodes()
    for node_id in alive_nodes[:2]:  # Fail first 2 nodes
        if node_id in master.node_objects:
            master.node_objects[node_id].shutdown()
            safe_print(f"💥 Forced failure of {node_id}")

    time.sleep(15)  # Wait for healing and generation

    # Test downloads
    safe_print("\n📋 Testing file integrity after failures...")
    for name, original_data in files.items():
        downloaded = client.download_file(name)
        if downloaded == original_data:
            safe_print(f"✅ {name} - OK")
        else:
            safe_print(f"❌ {name} - FAILED")

    # Show status
    safe_print(f"\n📊 Final status: {len(master.get_alive_nodes())} alive nodes, {len(master.files)} files")

    master.shutdown()

if __name__ == "__main__":
    simulate()