class DataNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.storage = {}  # file_name: data
        self.alive = True

    def store(self, file_name, data):
        if not self.alive:
            raise RuntimeError(f"Node {self.node_id} is down")
        self.storage[file_name] = data
        print(f"Node {self.node_id} stored {file_name}")

    def retrieve(self, file_name):
        if not self.alive:
            raise RuntimeError(f"Node {self.node_id} is down")
        if file_name not in self.storage:
            raise FileNotFoundError(f"File {file_name} not on node {self.node_id}")
        return self.storage[file_name]

    def fail(self):
        self.alive = False
        print(f"Node {self.node_id} has failed")

    def recover(self):
        self.alive = True
        print(f"Node {self.node_id} has recovered")


class NameNode:
    def __init__(self, data_nodes, replication_factor=3):
        self.data_nodes = data_nodes
        self.file_locations = {}  # file_name: list of node_ids
        self.replication_factor = replication_factor

    def _get_alive_nodes(self):
        return [node for node in self.data_nodes if node.alive]

    def create_file(self, file_name, data):
        alive_nodes = self._get_alive_nodes()
        if len(alive_nodes) < self.replication_factor:
            raise RuntimeError("Not enough alive nodes for replication")
        selected = alive_nodes[:self.replication_factor]
        for node in selected:
            node.store(file_name, data)
        self.file_locations[file_name] = [node.node_id for node in selected]
        print(f"File '{file_name}' created successfully with replication factor {self.replication_factor}")

    def read_file(self, file_name):
        if file_name not in self.file_locations:
            raise FileNotFoundError(f"File '{file_name}' not found")
        node_ids = self.file_locations[file_name][:]
        for nid in node_ids:
            node = next((n for n in self.data_nodes if n.node_id == nid), None)
            if node and node.alive:
                try:
                    data = node.retrieve(file_name)
                    print(f"Successfully read '{file_name}' from Node {nid}")
                    return data
                except Exception as e:
                    print(f"Failed to read from Node {nid}: {e}")
        raise RuntimeError(f"No available replica for '{file_name}' - possible data loss")

    def delete_file(self, file_name):
        if file_name not in self.file_locations:
            raise FileNotFoundError(f"File '{file_name}' not found")
        node_ids = self.file_locations[file_name]
        for nid in node_ids:
            node = next((n for n in self.data_nodes if n.node_id == nid), None)
            if node and node.alive:
                try:
                    if file_name in node.storage:
                        del node.storage[file_name]
                        print(f"Deleted {file_name} from Node {nid}")
                except:
                    pass
        del self.file_locations[file_name]
        print(f"File '{file_name}' deleted")

    def fail_node(self, node_id):
        node = next((n for n in self.data_nodes if n.node_id == node_id), None)
        if node:
            node.fail()
            self._re_replicate()

    def _re_replicate(self):
        print("Starting re-replication for fault tolerance...")
        for file_name in list(self.file_locations.keys()):
            locations = self.file_locations[file_name]
            live_nodes = [n for n in self.data_nodes if n.node_id in locations and n.alive]
            if len(live_nodes) == 0:
                print(f"WARNING: Data loss for '{file_name}' - no live replicas!")
                continue
            current_count = len(live_nodes)
            if current_count >= self.replication_factor:
                continue
            needed = self.replication_factor - current_count
            print(f"Re-replicating '{file_name}': {current_count}/{self.replication_factor} live replicas. Need {needed} more.")
            source_node = live_nodes[0]
            data = source_node.retrieve(file_name)
            new_locations = [n.node_id for n in live_nodes]
            available_nodes = [n for n in self.data_nodes if n.alive and n.node_id not in new_locations]
            new_selected = available_nodes[:needed]
            if len(new_selected) < needed:
                print(f"Not enough nodes to fully re-replicate '{file_name}'")
            for new_node in new_selected:
                new_node.store(file_name, data)
                new_locations.append(new_node.node_id)
            self.file_locations[file_name] = new_locations
            print(f"Re-replication complete for '{file_name}'. New replicas on: {new_locations}")


# =============== DEMO ===============
if __name__ == "__main__":
    print("=== Distributed File System Simulation with Fault Tolerance ===")
    nodes = [DataNode(i) for i in range(4)]          # 4 nodes total
    namenode = NameNode(nodes, replication_factor=3) # replicate each file 3 times

    print("Nodes initialized:", [n.node_id for n in nodes])

    namenode.create_file("example.txt", "This is distributed file system data with fault tolerance!")

    data = namenode.read_file("example.txt")
    print("File content:", data)

    print("\n=== Simulating node failure (Node 0) ===")
    namenode.fail_node(0)
    data = namenode.read_file("example.txt")
    print("File content after failure:", data)

    print("\n=== Simulating another node failure (Node 1) ===")
    namenode.fail_node(1)
    data = namenode.read_file("example.txt")
    print("File content after second failure:", data)

    print("\n=== Simulating third node failure (Node 2) ===")
    namenode.fail_node(2)
    data = namenode.read_file("example.txt")
    print("File content after third failure:", data)

    print("\n=== System Status: Data availability maintained via replication and automatic re-replication ===")