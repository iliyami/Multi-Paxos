import socket
import threading
from paxos import Paxos, Transaction
from datastore import Datastore

class Server:
    def __init__(self, server_id, host, port, total_servers, startup_barrier):
        self.id = server_id
        self.host = host
        self.port = port
        self.datastore = Datastore()
        self.paxos = Paxos(self)
        self.local_log = []
        self.client_balance = 100
        self.total_servers = total_servers
        self.quorum_size = (total_servers // 2) + 1
        self.live_servers = set(range(1, total_servers + 1))
        self.sequence_counter = 0
        self.startup_barrier = startup_barrier

    @staticmethod
    def get_server(server_id):
        import main
        return main.servers.get(server_id)


    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        print(f"Server {self.id} started on {self.host}:{self.port} ")
        self.startup_barrier.wait()
        
        while True:
            client_socket, addr = self.socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        # try:
            data = client_socket.recv(1024).decode()
            transaction = eval(data)
            result = self.process_transaction(transaction)
            client_socket.send(str(result).encode())
        # finally:
            # client_socket.close()

    def process_transaction(self, transaction):
        sender, receiver, amount = transaction
        if sender == self.id and self.client_balance >= amount:
            self.sequence_counter += 1
            new_transaction = Transaction(sender, receiver, amount, self.sequence_counter)
            self.local_log.append(new_transaction)
            self.client_balance -= amount
            return True
        elif sender == self.id:
            consensus_result = self.paxos.initiate_consensus(self.local_log)
            if consensus_result:
                return self.process_transaction(transaction)
            return False
        else:
            # Handle transactions for other clients
            return False

    def remove_committed_transactions(self, committed_transactions):
        self.local_log = [t for t in self.local_log if t not in committed_transactions]
        for t in committed_transactions:
            if t.sender == self.id:
                self.client_balance -= t.amount
            if t.receiver == self.id:
                self.client_balance += t.amount

    def get_live_servers(self):
        return [server for server in range(1, self.total_servers + 1) if server in self.live_servers]

    def print_balance(self):
        print(f"Balance of client {self.id}: {self.client_balance}")

    def print_log(self):
        print(f"Local log of server {self.id}:")
        for transaction in self.local_log:
            print(f"  {transaction}")

    def print_db(self):
        print(f"Datastore of server {self.id}:")
        for block in self.datastore.blocks:
            print(f"  Block {block.sequence_number}:")
            for transaction in block.transactions:
                print(f"    {transaction}")