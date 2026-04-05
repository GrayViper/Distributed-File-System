import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import random

class DataNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.storage = {}  # file_name: data
        self.alive = True

    def store(self, file_name, data):
        if not self.alive:
            raise RuntimeError(f"Node {self.node_id} is down")
        self.storage[file_name] = data

    def retrieve(self, file_name):
        if not self.alive:
            raise RuntimeError(f"Node {self.node_id} is down")
        if file_name not in self.storage:
            raise FileNotFoundError(f"File not on node {self.node_id}")
        return self.storage[file_name]


class NameNode:
    def __init__(self, data_nodes, replication_factor=3):
        self.data_nodes = data_nodes
        self.file_locations = {}  # file_name: list of node_ids
        self.replication_factor = replication_factor

    def _get_alive_nodes(self):
        return [n for n in self.data_nodes if n.alive]

    def create_file(self, file_name, data):
        alive = self._get_alive_nodes()
        if len(alive) < self.replication_factor:
            raise RuntimeError("Not enough alive nodes")
        selected = alive[:self.replication_factor]
        for node in selected:
            node.store(file_name, data)
        self.file_locations[file_name] = [n.node_id for n in selected]
        return f"File '{file_name}' created (replicated on {len(selected)} nodes)"

    def read_file(self, file_name):
        if file_name not in self.file_locations:
            raise FileNotFoundError(f"File '{file_name}' not found")
        node_ids = self.file_locations[file_name][:]
        random.shuffle(node_ids)  # Try replicas in random order
        for nid in node_ids:
            node = next((n for n in self.data_nodes if n.node_id == nid), None)
            if node and node.alive:
                try:
                    return node.retrieve(file_name)
                except:
                    continue
        raise RuntimeError("No available replica")

    def delete_file(self, file_name):
        if file_name not in self.file_locations:
            raise FileNotFoundError(f"File '{file_name}' not found")
        for nid in self.file_locations[file_name]:
            node = next((n for n in self.data_nodes if n.node_id == nid), None)
            if node and node.alive and file_name in node.storage:
                del node.storage[file_name]
        del self.file_locations[file_name]
        return f"File '{file_name}' deleted"

    def fail_node(self, node_id):
        node = next((n for n in self.data_nodes if n.node_id == node_id), None)
        if node:
            node.alive = False
            self._re_replicate()

    def recover_node(self, node_id):
        node = next((n for n in self.data_nodes if n.node_id == node_id), None)
        if node:
            node.alive = True

    def _re_replicate(self):
        for fname in list(self.file_locations.keys()):
            locations = self.file_locations[fname]
            live = [n for n in self.data_nodes if n.node_id in locations and n.alive]
            if not live:
                continue
            current = len(live)
            if current >= self.replication_factor:
                continue
            needed = self.replication_factor - current
            data = live[0].retrieve(fname)
            available = [n for n in self.data_nodes if n.alive and n.node_id not in [l.node_id for l in live]]
            new_nodes = available[:needed]
            for nn in new_nodes:
                nn.store(fname, data)
                locations.append(nn.node_id)
            self.file_locations[fname] = locations


class DFS_GUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Mini Distributed File System with Fault Tolerance")
        self.root.geometry("900x700")

        # Core system
        self.nodes = [DataNode(i) for i in range(5)]  # 5 small nodes
        self.namenode = NameNode(self.nodes, replication_factor=3)

        self.create_widgets()
        self.update_node_status()

    def create_widgets(self):
        # Title
        tk.Label(self.root, text="Mini DFS - Fault Tolerant", font=("Arial", 16, "bold")).pack(pady=10)

        # Node Status Frame
        status_frame = tk.LabelFrame(self.root, text="Data Nodes Status", padx=10, pady=10)
        status_frame.pack(fill="x", padx=10, pady=5)

        self.node_buttons = []
        for i in range(len(self.nodes)):
            btn = tk.Button(status_frame, text=f"Node {i}", width=12, command=lambda x=i: self.toggle_node(x))
            btn.pack(side="left", padx=5)
            self.node_buttons.append(btn)

        # File Operations
        op_frame = tk.LabelFrame(self.root, text="File Operations", padx=10, pady=10)
        op_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(op_frame, text="Filename:").grid(row=0, column=0, sticky="w")
        self.filename_entry = tk.Entry(op_frame, width=30)
        self.filename_entry.grid(row=0, column=1, padx=5)

        tk.Label(op_frame, text="Content:").grid(row=1, column=0, sticky="w")
        self.content_entry = tk.Text(op_frame, width=40, height=3)
        self.content_entry.grid(row=1, column=1, padx=5, pady=5)

        btn_frame = tk.Frame(op_frame)
        btn_frame.grid(row=2, column=1, pady=5)

        tk.Button(btn_frame, text="Create File", bg="#4CAF50", fg="white", command=self.create_file).pack(side="left", padx=5)
        tk.Button(btn_frame, text="Read File", bg="#2196F3", fg="white", command=self.read_file).pack(side="left", padx=5)
        tk.Button(btn_frame, text="Delete File", bg="#f44336", fg="white", command=self.delete_file).pack(side="left", padx=5)

        # Log
        log_frame = tk.LabelFrame(self.root, text="System Log", padx=10, pady=10)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, state='disabled')
        self.log_text.pack(fill="both", expand=True)

        # Summary
        self.summary_label = tk.Label(self.root, text="", font=("Arial", 10))
        self.summary_label.pack(pady=5)

    def log(self, message):
        self.log_text.configure(state='normal')
        self.log_text.insert(tk.END, f"[{tk.datetime.datetime.now().strftime('%H:%M:%S')}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state='disabled')

    def update_node_status(self):
        for i, btn in enumerate(self.node_buttons):
            node = self.nodes[i]
            color = "green" if node.alive else "red"
            btn.config(bg=color, fg="white", text=f"Node {i} {'✓' if node.alive else '✗'}")
        alive_count = sum(1 for n in self.nodes if n.alive)
        self.summary_label.config(text=f"Alive Nodes: {alive_count}/5 | Replication Factor: 3")

    def toggle_node(self, node_id):
        node = self.nodes[node_id]
        if node.alive:
            self.namenode.fail_node(node_id)
            self.log(f"Node {node_id} failed manually")
        else:
            self.namenode.recover_node(node_id)
            self.log(f"Node {node_id} recovered")
        self.update_node_status()

    def create_file(self):
        fname = self.filename_entry.get().strip()
        content = self.content_entry.get("1.0", tk.END).strip()
        if not fname or not content:
            messagebox.showwarning("Input Error", "Filename and content required")
            return
        try:
            msg = self.namenode.create_file(fname, content)
            self.log(msg)
            messagebox.showinfo("Success", msg)
            self.filename_entry.delete(0, tk.END)
            self.content_entry.delete("1.0", tk.END)
        except Exception as e:
            messagebox.showerror("Error", str(e))
        self.update_node_status()

    def read_file(self):
        fname = self.filename_entry.get().strip()
        if not fname:
            messagebox.showwarning("Input Error", "Enter filename to read")
            return
        try:
            data = self.namenode.read_file(fname)
            self.log(f"Read '{fname}' successfully")
            messagebox.showinfo("File Content", f"Filename: {fname}\n\nContent:\n{data}")
        except Exception as e:
            messagebox.showerror("Read Error", str(e))
            self.log(f"Read failed for '{fname}': {e}")

    def delete_file(self):
        fname = self.filename_entry.get().strip()
        if not fname:
            messagebox.showwarning("Input Error", "Enter filename to delete")
            return
        try:
            msg = self.namenode.delete_file(fname)
            self.log(msg)
            messagebox.showinfo("Success", msg)
            self.filename_entry.delete(0, tk.END)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def run(self):
        self.log("Mini DFS GUI started. 5 nodes, replication factor = 3")
        self.root.mainloop()


if __name__ == "__main__":
    app = DFS_GUI()
    app.run()