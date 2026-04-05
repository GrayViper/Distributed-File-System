import threading
import time
import random
import hashlib
import json
import os
from collections import defaultdict
from typing import Dict, List, Set, Optional, Tuple

# -------------------------------
# GLOBAL CONFIGURATION
# -------------------------------
REPLICATION_FACTOR = 3
CHUNK_SIZE = 1024  # 1KB chunks
HEARTBEAT_INTERVAL = 2  # seconds
NODE_TIMEOUT = 10  # seconds
MAX_RETRIES = 3

# -------------------------------
# THREAD-SAFE PRINTING
# -------------------------------
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

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
        self.replicas: Set[str] = set()  # node_ids that have this chunk

    def to_dict(self):
        return {
            'chunk_id': self.chunk_id,
            'file_name': self.file_name,
            'chunk_index': self.chunk_index,
            'size': self.size,
            'checksum': self.checksum,
            'replicas': list(self.replicas)
        }

    @classmethod
    def from_dict(cls, data):
        chunk = cls(
            data['chunk_id'],
            data['file_name'],
            data['chunk_index'],
            data['size'],
            data['checksum']
        )
        chunk.replicas = set(data['replicas'])
        return chunk

class FileMetadata:
    def __init__(self, file_name: str, total_size: int, total_chunks: int, created_time: float):
        self.file_name = file_name
        self.total_size = total_size
        self.total_chunks = total_chunks
        self.created_time = created_time
        self.chunks: List[str] = []  # list of chunk_ids

    def to_dict(self):
        return {
            'file_name': self.file_name,
            'total_size': self.total_size,
            'total_chunks': self.total_chunks,
            'created_time': self.created_time,
            'chunks': self.chunks
        }

    @classmethod
    def from_dict(cls, data):
        file_meta = cls(
            data['file_name'],
            data['total_size'],
            data['total_chunks'],
            data['created_time']
        )
        file_meta.chunks = data['chunks']
        return file_meta

# -------------------------------
# MASTER NODE (NAME NODE)
# -------------------------------
class MasterNode:
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}  # node_id -> node_info
        self.files: Dict[str, FileMetadata] = {}  # file_name -> FileMetadata
        self.chunks: Dict[str, ChunkMetadata] = {}  # chunk_id -> ChunkMetadata
        self.lock = threading.Lock()
        self.running = True

        # Start background threads
        threading.Thread(target=self._monitor_nodes, daemon=True).start()
        threading.Thread(target=self._rebalance_data, daemon=True).start()

    def register_node(self, node_id: str, node_info: Dict):
        """Register or update node information"""
        with self.lock:
            self.nodes[node_id] = {
                **node_info,
                'last_heartbeat': time.time(),
                'status': 'alive'
            }
            safe_print(f"📝 Node {node_id} registered/updated")

    def heartbeat(self, node_id: str, node_info: Dict):
        """Receive heartbeat from data node"""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].update({
                    **node_info,
                    'last_heartbeat': time.time(),
                    'status': 'alive'
                })

    def get_alive_nodes(self) -> List[str]:
        """Get list of currently alive nodes"""
        current_time = time.time()
        with self.lock:
            return [node_id for node_id, info in self.nodes.items()
                   if current_time - info.get('last_heartbeat', 0) < NODE_TIMEOUT]

    def get_node_score(self, node_info: Dict) -> float:
        """Calculate node score for load balancing"""
        cpu_usage = node_info.get('cpu_usage', 50)
        free_space = node_info.get('free_space', 1000)
        active_requests = node_info.get('active_requests', 0)

        # Higher score = better node
        return (free_space * 0.5) - (cpu_usage * 0.3) - (active_requests * 0.2)

    def select_replica_nodes(self, exclude_nodes: Set[str] = None) -> List[str]:
        """Select best nodes for replication"""
        if exclude_nodes is None:
            exclude_nodes = set()

        alive_nodes = [n for n in self.get_alive_nodes() if n not in exclude_nodes]

        if len(alive_nodes) < REPLICATION_FACTOR:
            raise RuntimeError(f"Not enough alive nodes: {len(alive_nodes)}/{REPLICATION_FACTOR}")

        # Sort by score (highest first)
        scored_nodes = [(node_id, self.get_node_score(self.nodes[node_id]))
                       for node_id in alive_nodes]
        scored_nodes.sort(key=lambda x: x[1], reverse=True)

        return [node_id for node_id, _ in scored_nodes[:REPLICATION_FACTOR]]

    def store_file(self, file_name: str, data: bytes) -> bool:
        """Store file by chunking and replicating"""
        try:
            # Check if file already exists
            if file_name in self.files:
                safe_print(f"⚠️ File {file_name} already exists")
                return False

            # Create chunks
            chunks = self._create_chunks(data)
            chunk_ids = []

            with self.lock:
                # Store file metadata
                file_meta = FileMetadata(
                    file_name=file_name,
                    total_size=len(data),
                    total_chunks=len(chunks),
                    created_time=time.time()
                )
                self.files[file_name] = file_meta

                # Store each chunk
                for i, chunk_data in enumerate(chunks):
                    chunk_id = f"{file_name}_chunk_{i}"
                    checksum = self._calculate_checksum(chunk_data)

                    chunk_meta = ChunkMetadata(
                        chunk_id=chunk_id,
                        file_name=file_name,
                        chunk_index=i,
                        size=len(chunk_data),
                        checksum=checksum
                    )
                    self.chunks[chunk_id] = chunk_meta
                    chunk_ids.append(chunk_id)

                    # Select replica nodes
                    replica_nodes = self.select_replica_nodes()

                    # Store on replica nodes
                    success_count = 0
                    for node_id in replica_nodes:
                        if self._store_chunk_on_node(node_id, chunk_id, chunk_data):
                            chunk_meta.replicas.add(node_id)
                            success_count += 1

                    if success_count < REPLICATION_FACTOR:
                        safe_print(f"⚠️ Failed to achieve full replication for chunk {chunk_id}")
                        # Continue anyway - system can re-replicate later

                file_meta.chunks = chunk_ids

            safe_print(f"✅ File {file_name} stored successfully ({len(chunks)} chunks)")
            return True

        except Exception as e:
            safe_print(f"❌ Failed to store file {file_name}: {e}")
            # Cleanup on failure
            self._cleanup_failed_file(file_name)
            return False

    def retrieve_file(self, file_name: str) -> Optional[bytes]:
        """Retrieve complete file from chunks"""
        try:
            with self.lock:
                if file_name not in self.files:
                    safe_print(f"❌ File {file_name} not found")
                    return None

                file_meta = self.files[file_name]
                chunks_data = []

                for chunk_id in file_meta.chunks:
                    if chunk_id not in self.chunks:
                        safe_print(f"❌ Chunk {chunk_id} metadata missing")
                        return None

                    chunk_meta = self.chunks[chunk_id]
                    chunk_data = self._retrieve_chunk(chunk_id)

                    if chunk_data is None:
                        safe_print(f"❌ Failed to retrieve chunk {chunk_id}")
                        return None

                    # Verify checksum
                    if self._calculate_checksum(chunk_data) != chunk_meta.checksum:
                        safe_print(f"❌ Checksum mismatch for chunk {chunk_id}")
                        return None

                    chunks_data.append(chunk_data)

                # Reassemble file
                complete_data = b''.join(chunks_data)
                safe_print(f"✅ File {file_name} retrieved successfully")
                return complete_data

        except Exception as e:
            safe_print(f"❌ Failed to retrieve file {file_name}: {e}")
            return None

    def delete_file(self, file_name: str) -> bool:
        """Delete file and all its chunks"""
        try:
            with self.lock:
                if file_name not in self.files:
                    safe_print(f"⚠️ File {file_name} not found")
                    return False

                file_meta = self.files[file_name]

                # Delete chunks from all nodes
                for chunk_id in file_meta.chunks:
                    if chunk_id in self.chunks:
                        chunk_meta = self.chunks[chunk_id]
                        for node_id in list(chunk_meta.replicas):
                            self._delete_chunk_from_node(node_id, chunk_id)
                        del self.chunks[chunk_id]

                del self.files[file_name]

            safe_print(f"🗑️ File {file_name} deleted successfully")
            return True

        except Exception as e:
            safe_print(f"❌ Failed to delete file {file_name}: {e}")
            return False

    def _create_chunks(self, data: bytes) -> List[bytes]:
        """Split data into chunks"""
        return [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]

    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate MD5 checksum"""
        return hashlib.md5(data).hexdigest()

    def _store_chunk_on_node(self, node_id: str, chunk_id: str, data: bytes) -> bool:
        """Store chunk on specific node"""
        # In a real implementation, this would send data over network
        # For simulation, we'll assume nodes have a store_chunk method
        return True  # Placeholder

    def _retrieve_chunk(self, chunk_id: str) -> Optional[bytes]:
        """Retrieve chunk from available replica"""
        chunk_meta = self.chunks[chunk_id]

        # Try each replica in random order
        replica_nodes = list(chunk_meta.replicas)
        random.shuffle(replica_nodes)

        for node_id in replica_nodes:
            if node_id in self.get_alive_nodes():
                # In real implementation, fetch from node
                # For simulation, return mock data
                return b"mock_chunk_data"  # Placeholder

        return None

    def _delete_chunk_from_node(self, node_id: str, chunk_id: str) -> bool:
        """Delete chunk from specific node"""
        # Placeholder for network operation
        return True

    def _cleanup_failed_file(self, file_name: str):
        """Clean up partial file storage on failure"""
        try:
            if file_name in self.files:
                file_meta = self.files[file_name]
                for chunk_id in file_meta.chunks:
                    if chunk_id in self.chunks:
                        chunk_meta = self.chunks[chunk_id]
                        for node_id in list(chunk_meta.replicas):
                            self._delete_chunk_from_node(node_id, chunk_id)
                        del self.chunks[chunk_id]
                del self.files[file_name]
        except Exception as e:
            safe_print(f"⚠️ Cleanup failed for {file_name}: {e}")

    def _monitor_nodes(self):
        """Monitor node health and handle failures"""
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            current_time = time.time()

            with self.lock:
                dead_nodes = []
                for node_id, node_info in self.nodes.items():
                    if current_time - node_info.get('last_heartbeat', 0) > NODE_TIMEOUT:
                        if node_info.get('status') == 'alive':
                            safe_print(f"💀 Node {node_id} detected as dead")
                            node_info['status'] = 'dead'
                            dead_nodes.append(node_id)

                # Trigger re-replication for dead nodes
                for dead_node in dead_nodes:
                    self._handle_node_failure(dead_node)

    def _handle_node_failure(self, dead_node: str):
        """Handle node failure by re-replicating data"""
        safe_print(f"🔄 Starting re-replication for failed node {dead_node}")

        affected_chunks = []
        with self.lock:
            for chunk_id, chunk_meta in self.chunks.items():
                if dead_node in chunk_meta.replicas:
                    chunk_meta.replicas.discard(dead_node)
                    if len(chunk_meta.replicas) < REPLICATION_FACTOR:
                        affected_chunks.append(chunk_id)

        # Re-replicate affected chunks
        for chunk_id in affected_chunks:
            self._re_replicate_chunk(chunk_id)

    def _re_replicate_chunk(self, chunk_id: str):
        """Re-replicate a chunk to maintain replication factor"""
        try:
            chunk_meta = self.chunks[chunk_id]

            if len(chunk_meta.replicas) >= REPLICATION_FACTOR:
                return  # Already sufficiently replicated

            needed = REPLICATION_FACTOR - len(chunk_meta.replicas)

            # Get chunk data from existing replica
            chunk_data = self._retrieve_chunk(chunk_id)
            if chunk_data is None:
                safe_print(f"❌ Cannot re-replicate {chunk_id}: no source data")
                return

            # Select new replica nodes
            exclude_nodes = chunk_meta.replicas
            try:
                new_nodes = self.select_replica_nodes(exclude_nodes)[:needed]
            except RuntimeError:
                safe_print(f"⚠️ Not enough nodes to fully re-replicate {chunk_id}")
                new_nodes = self.get_alive_nodes()
                new_nodes = [n for n in new_nodes if n not in exclude_nodes][:needed]

            # Store on new nodes
            for node_id in new_nodes:
                if self._store_chunk_on_node(node_id, chunk_id, chunk_data):
                    chunk_meta.replicas.add(node_id)
                    safe_print(f"🔄 Re-replicated {chunk_id} to node {node_id}")

        except Exception as e:
            safe_print(f"❌ Failed to re-replicate chunk {chunk_id}: {e}")

    def _rebalance_data(self):
        """Periodically rebalance data distribution"""
        while self.running:
            time.sleep(30)  # Rebalance every 30 seconds

            try:
                with self.lock:
                    alive_nodes = self.get_alive_nodes()
                    if len(alive_nodes) < REPLICATION_FACTOR:
                        continue  # Not enough nodes for rebalancing

                    # Check for under-replicated chunks
                    under_replicated = []
                    for chunk_id, chunk_meta in self.chunks.items():
                        if len(chunk_meta.replicas) < REPLICATION_FACTOR:
                            under_replicated.append(chunk_id)

                    for chunk_id in under_replicated:
                        self._re_replicate_chunk(chunk_id)

            except Exception as e:
                safe_print(f"⚠️ Rebalancing error: {e}")

    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        with self.lock:
            alive_nodes = self.get_alive_nodes()
            total_nodes = len(self.nodes)

            chunk_distribution = defaultdict(int)
            for chunk_meta in self.chunks.values():
                replica_count = len(chunk_meta.replicas)
                chunk_distribution[replica_count] += 1

            return {
                'total_nodes': total_nodes,
                'alive_nodes': len(alive_nodes),
                'dead_nodes': total_nodes - len(alive_nodes),
                'total_files': len(self.files),
                'total_chunks': len(self.chunks),
                'chunk_distribution': dict(chunk_distribution),
                'replication_factor': REPLICATION_FACTOR
            }

    def shutdown(self):
        """Shutdown the master node"""
        self.running = False
        safe_print("🛑 Master node shutting down")

# -------------------------------
# DATA NODE
# -------------------------------
class DataNode:
    def __init__(self, node_id: str, master: MasterNode):
        self.node_id = node_id
        self.master = master
        self.storage: Dict[str, bytes] = {}  # chunk_id -> data
        self.active_requests = 0
        self.running = True

        # Simulate node resources
        self.cpu_usage = random.randint(10, 90)
        self.free_space = random.randint(500, 2000)

        # Start heartbeat thread
        threading.Thread(target=self._send_heartbeats, daemon=True).start()

    def _send_heartbeats(self):
        """Send periodic heartbeats to master"""
        while self.running:
            try:
                node_info = {
                    'cpu_usage': self.cpu_usage,
                    'free_space': self.free_space,
                    'active_requests': self.active_requests,
                    'total_chunks': len(self.storage)
                }
                self.master.heartbeat(self.node_id, node_info)
            except Exception as e:
                safe_print(f"⚠️ Heartbeat failed for node {self.node_id}: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def store_chunk(self, chunk_id: str, data: bytes) -> bool:
        """Store a chunk"""
        try:
            self.active_requests += 1
            time.sleep(random.uniform(0.01, 0.1))  # Simulate I/O delay

            self.storage[chunk_id] = data
            self.free_space -= len(data) // 1024  # Rough space calculation

            safe_print(f"💾 Node {self.node_id} stored chunk {chunk_id}")
            return True

        except Exception as e:
            safe_print(f"❌ Node {self.node_id} failed to store chunk {chunk_id}: {e}")
            return False
        finally:
            self.active_requests -= 1

    def retrieve_chunk(self, chunk_id: str) -> Optional[bytes]:
        """Retrieve a chunk"""
        try:
            self.active_requests += 1
            time.sleep(random.uniform(0.01, 0.05))  # Simulate I/O delay

            if chunk_id not in self.storage:
                return None

            data = self.storage[chunk_id]
            safe_print(f"📖 Node {self.node_id} retrieved chunk {chunk_id}")
            return data

        except Exception as e:
            safe_print(f"❌ Node {self.node_id} failed to retrieve chunk {chunk_id}: {e}")
            return None
        finally:
            self.active_requests -= 1

    def delete_chunk(self, chunk_id: str) -> bool:
        """Delete a chunk"""
        try:
            if chunk_id in self.storage:
                data_size = len(self.storage[chunk_id])
                del self.storage[chunk_id]
                self.free_space += data_size // 1024
                safe_print(f"🗑️ Node {self.node_id} deleted chunk {chunk_id}")
                return True
            return False
        except Exception as e:
            safe_print(f"❌ Node {self.node_id} failed to delete chunk {chunk_id}: {e}")
            return False

    def fail(self):
        """Simulate node failure"""
        safe_print(f"💥 Node {self.node_id} FAILED")
        self.running = False

    def recover(self):
        """Simulate node recovery"""
        safe_print(f"🔄 Node {self.node_id} RECOVERING")
        self.running = True
        self.cpu_usage = random.randint(10, 90)
        self.free_space = random.randint(500, 2000)

        # Re-register with master
        node_info = {
            'cpu_usage': self.cpu_usage,
            'free_space': self.free_space,
            'active_requests': self.active_requests
        }
        self.master.register_node(self.node_id, node_info)

        # Start heartbeat again
        threading.Thread(target=self._send_heartbeats, daemon=True).start()

# -------------------------------
# CLIENT INTERFACE
# -------------------------------
class DFSClient:
    def __init__(self, master: MasterNode):
        self.master = master

    def upload_file(self, file_name: str, data: bytes) -> bool:
        """Upload a file to the DFS"""
        safe_print(f"📤 Uploading file: {file_name} ({len(data)} bytes)")
        return self.master.store_file(file_name, data)

    def download_file(self, file_name: str) -> Optional[bytes]:
        """Download a file from the DFS"""
        safe_print(f"📥 Downloading file: {file_name}")
        return self.master.retrieve_file(file_name)

    def delete_file(self, file_name: str) -> bool:
        """Delete a file from the DFS"""
        safe_print(f"🗑️ Deleting file: {file_name}")
        return self.master.delete_file(file_name)

    def list_files(self) -> List[str]:
        """List all files in the DFS"""
        return list(self.master.files.keys())

    def get_system_status(self) -> Dict:
        """Get system status"""
        return self.master.get_system_status()

# -------------------------------
# SIMULATION AND TESTING
# -------------------------------
def simulate_distributed_file_system():
    """Run a comprehensive simulation of the distributed file system"""
    safe_print("🚀 Starting Distributed File System Simulation")
    safe_print("=" * 60)

    # Initialize master node
    master = MasterNode()

    # Initialize data nodes
    nodes = {}
    for i in range(5):
        node_id = f"node_{i}"
        node = DataNode(node_id, master)
        nodes[node_id] = node

        # Register node with master
        node_info = {
            'cpu_usage': node.cpu_usage,
            'free_space': node.free_space,
            'active_requests': 0
        }
        master.register_node(node_id, node_info)

    # Initialize client
    client = DFSClient(master)

    # Wait for system to stabilize
    time.sleep(2)

    safe_print("\n📊 Initial System Status:")
    status = client.get_system_status()
    safe_print(json.dumps(status, indent=2))

    # Test 1: Upload files
    safe_print("\n📤 Test 1: Uploading files")
    test_files = {
        "document.txt": b"This is a test document for the distributed file system.",
        "data.json": b'{"key": "value", "numbers": [1, 2, 3, 4, 5]}',
        "large_file.bin": b"A" * 5000,  # 5KB file
        "code.py": b"print('Hello, Distributed World!')"
    }

    for file_name, data in test_files.items():
        success = client.upload_file(file_name, data)
        if not success:
            safe_print(f"❌ Failed to upload {file_name}")

    time.sleep(1)

    # Test 2: Download files
    safe_print("\n📥 Test 2: Downloading files")
    for file_name in test_files.keys():
        data = client.download_file(file_name)
        if data and data == test_files[file_name]:
            safe_print(f"✅ {file_name} downloaded successfully")
        else:
            safe_print(f"❌ {file_name} download failed or data corrupted")

    # Test 3: Simulate node failures
    safe_print("\n💥 Test 3: Simulating node failures")
    status = client.get_system_status()
    safe_print(f"System status before failures: {status['alive_nodes']}/{status['total_nodes']} nodes alive")

    # Fail two nodes
    nodes["node_1"].fail()
    time.sleep(3)  # Wait for failure detection

    nodes["node_3"].fail()
    time.sleep(3)  # Wait for failure detection and re-replication

    status = client.get_system_status()
    safe_print(f"System status after failures: {status['alive_nodes']}/{status['total_nodes']} nodes alive")
    safe_print(f"Chunk distribution: {status['chunk_distribution']}")

    # Test 4: Verify data availability after failures
    safe_print("\n🔍 Test 4: Verifying data availability after failures")
    for file_name in test_files.keys():
        data = client.download_file(file_name)
        if data and data == test_files[file_name]:
            safe_print(f"✅ {file_name} still accessible after node failures")
        else:
            safe_print(f"❌ {file_name} lost due to node failures")

    # Test 5: Node recovery
    safe_print("\n🔄 Test 5: Simulating node recovery")
    nodes["node_1"].recover()
    time.sleep(2)

    status = client.get_system_status()
    safe_print(f"System status after recovery: {status['alive_nodes']}/{status['total_nodes']} nodes alive")

    # Test 6: Upload new file after recovery
    safe_print("\n📤 Test 6: Uploading new file after recovery")
    new_file_data = b"This is a new file uploaded after node recovery."
    success = client.upload_file("recovery_test.txt", new_file_data)
    if success:
        retrieved_data = client.download_file("recovery_test.txt")
        if retrieved_data == new_file_data:
            safe_print("✅ New file upload and retrieval successful")
        else:
            safe_print("❌ New file data corrupted")
    else:
        safe_print("❌ Failed to upload new file")

    # Test 7: Delete files
    safe_print("\n🗑️ Test 7: Deleting files")
    for file_name in ["document.txt", "recovery_test.txt"]:
        success = client.delete_file(file_name)
        if success:
            safe_print(f"✅ {file_name} deleted successfully")
        else:
            safe_print(f"❌ Failed to delete {file_name}")

    # Final status
    safe_print("\n📊 Final System Status:")
    status = client.get_system_status()
    safe_print(json.dumps(status, indent=2))

    # Shutdown
    master.shutdown()
    safe_print("\n🛑 Simulation completed")

if __name__ == "__main__":
    simulate_distributed_file_system()