import threading
import time
import random

# -------------------------
# GLOBAL PRINT LOCK
# -------------------------
print_lock = threading.Lock()

def safe_print(*args):
    with print_lock:
        print(*args)

# -------------------------
# MASTER (Coordinator)
# -------------------------
class Master:
    def __init__(self):
        self.nodes = {}
        self.chunks = {}
        self.lock = threading.Lock()

    def heartbeat(self, node_id, cpu, free_space, requests):
        with self.lock:
            self.nodes[node_id] = {
                "cpu": cpu,
                "free_space": free_space,
                "requests": requests,
                "last_seen": time.time()
            }

    def node_score(self, node):
        return (
            node["free_space"] * 0.5
            - node["cpu"] * 0.3
            - node["requests"] * 0.2
        )

    def get_alive_nodes(self):
        with self.lock:
            return {
                k: v for k, v in self.nodes.items()
                if time.time() - v["last_seen"] < 5
            }

    def get_best_nodes(self, n=2):
        alive = self.get_alive_nodes()
        sorted_nodes = sorted(
            alive.items(),
            key=lambda x: self.node_score(x[1]),
            reverse=True
        )
        return [node_id for node_id, _ in sorted_nodes[:n]]

    def register_chunk(self, chunk, node):
        with self.lock:
            self.chunks.setdefault(chunk, []).append(node)

    # 🔥 SELF-HEALING (REAL DATA COPY)
    def recover_chunks(self, dead_node, storage_nodes):
        with self.lock:
            for chunk, nodes in list(self.chunks.items()):

                if dead_node in nodes:
                    nodes.remove(dead_node)

                # If replication broken
                if len(nodes) < 2 and len(nodes) > 0:

                    source_node_id = nodes[0]
                    source_node = storage_nodes[source_node_id]

                    candidates = self.get_best_nodes(3)

                    for new_node_id in candidates:
                        if new_node_id not in nodes:
                            target_node = storage_nodes[new_node_id]

                            data = source_node.storage.get(chunk)

                            if data:
                                target_node.storage[chunk] = data
                                nodes.append(new_node_id)

                                safe_print(f"♻️ Re-replicated {chunk} from {source_node_id} → {new_node_id}")
                                safe_print("Recovering:", chunk, "Current:", nodes)
                                break

    def show_status(self):
        alive = self.get_alive_nodes()
        with print_lock:
            print("\n--- MASTER STATUS ---")
            print("Alive Nodes:", alive)
            print("Chunks:", self.chunks)


# -------------------------
# STORAGE NODE
# -------------------------
class StorageNode:
    def __init__(self, node_id, master):
        self.node_id = node_id
        self.master = master
        self.storage = {}
        self.active_requests = 0
        self.running = True

        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def send_heartbeat(self):
        while self.running:
            cpu = random.randint(1, 100)
            free_space = random.randint(100, 1000)

            self.master.heartbeat(
                self.node_id,
                cpu,
                free_space,
                self.active_requests
            )
            time.sleep(1)

    def store_chunk(self, chunk_name, data):
        self.active_requests += 1

        time.sleep(random.uniform(0.1, 0.5))
        self.storage[chunk_name] = data

        self.master.register_chunk(chunk_name, self.node_id)

        self.active_requests -= 1

    def fail(self):
        safe_print(f"💥 Node {self.node_id} FAILED")
        self.running = False

        # Mark as dead
        self.master.nodes[self.node_id]["last_seen"] = 0

        # Trigger self-healing
        self.master.recover_chunks(self.node_id, ALL_NODES)


# -------------------------
# CLIENT
# -------------------------
class Client:
    def __init__(self, master, nodes):
        self.master = master
        self.nodes = nodes

    def upload(self, filename, data, replication=2):
        chunk_size = 5
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

        for i, chunk in enumerate(chunks):
            chunk_name = f"{filename}_chunk_{i}"

            best_nodes = self.master.get_best_nodes(replication)

            for node_id in best_nodes:
                node = self.nodes[node_id]
                threading.Thread(
                    target=node.store_chunk,
                    args=(chunk_name, chunk)
                ).start()


# -------------------------
# SIMULATION
# -------------------------
if __name__ == "__main__":
    master = Master()

    # Create nodes
    nodes = {
        f"node{i}": StorageNode(f"node{i}", master)
        for i in range(3)
    }

    # 🔥 Global reference for recovery
    ALL_NODES = nodes

    client = Client(master, nodes)

    # Upload file
    client.upload("file1", "THIS_IS_A_DISTRIBUTED_FILE_SYSTEM_SIMULATION")

    time.sleep(3)
    master.show_status()

    # Simulate failure
    nodes["node1"].fail()

    safe_print("\nUploading again after failure...\n")

    client.upload("file2", "FAULT_TOLERANCE_TEST_DATA")

    time.sleep(3)
    master.show_status()