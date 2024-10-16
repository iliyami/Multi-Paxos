import socket
import threading
import json
from queue import Queue
import time
import csv

# Constants
INITIAL_BALANCE = 100
NUM_SERVERS = 3
MAJORITY = NUM_SERVERS // 2 + 1  # Paxos requires a majority to commit

# Sample server class handling TCP connections and Paxos protocol
class PaxosServer:
    def __init__(self, server_id, port, peers):
        self.server_id = server_id
        self.port = port
        self.peers = peers  # List of peer server ports
        self.transactions_log = []  # Local log of transactions
        self.datastore = []  # List of committed blocks
        self.balance = INITIAL_BALANCE  # Initial balance for this server
        self.is_leader = False
        self.ballot_number = 0
        self.promised_number = 0  # For promise phase
        self.accepted_value = None
        self.accepted_number = 0
        self.majority_responses = 0  # Count for majority
        self.local_major_block = []
        self.lock = threading.Lock()  # For thread safety
        self.last_committed_block = (0, 0)
        self.pending_transaction = None
        
        self.prepare_queue = Queue()

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('localhost', self.port))
        server_socket.listen(5)
        print(f"Server {self.server_id} started on port {self.port}")
        
        # Start a thread for accepting client connections
        threading.Thread(target=self.accept_connections, args=(server_socket,)).start()

    def accept_connections(self, server_socket):
        while True:
            client_conn, _ = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_conn,)).start()

    def handle_client(self, conn):
        data = conn.recv(1024).decode()
        request = json.loads(data)
        if 'transaction' in request:
            self.handle_transaction(request['transaction'])
        elif 'paxos' in request:
            self.handle_paxos_message(request['paxos'])

    # ------------- Paxos Phases ----------------- #
    
    def handle_transaction(self, transaction):
        seq_num, trans = transaction
        sender, receiver, amount = trans
        # If balance is insufficient, initiate Paxos protocol
        if self.balance < amount:
            print(f"Server {self.server_id}: Insufficient funds, initiating Paxos for transaction {transaction}")
            self.pending_transaction = transaction
            self.initiate_paxos(transaction)
        else:
            # Process the transaction locally and update log
            self.balance -= amount
            self.transactions_log.append(transaction)
            print(f"Server {self.server_id}: Processed transaction {transaction}")

    def initiate_paxos(self, transaction):
        self.pending_paxos = True
        self.ballot_number += 1
        # Send PREPARE message to all peers
        for peer_port in self.peers:
            self.send_prepare(peer_port)

    def send_prepare(self, peer_port):
        #TODO last commited block number
        message = {'paxos': {
            'type': 'prepare',
            'ballot_number': self.ballot_number,
            'sender_id': self.server_id,
            'last_committed_block': self.last_committed_block,
        }}
        self.send_message(peer_port, message)

    def handle_paxos_message(self, paxos_message):
        paxos_type = paxos_message['type']
        if paxos_type == 'prepare':
            self.handle_prepare(paxos_message)
        elif paxos_type == 'promise':
            self.handle_promise(paxos_message)
        elif paxos_type == 'accept':
            self.handle_accept(paxos_message)
        elif paxos_type == 'accepted':
            self.handle_accepted(paxos_message)
        elif paxos_type == 'commit':
            self.handle_commit(paxos_message)
        elif paxos_type == 'catch_up_request':
            self.handle_catch_up_request(paxos_message)
        elif paxos_type == 'catch_up_response':
            self.handle_catch_up_response(paxos_message)

    def handle_prepare(self, message):
        ballot_number = message['ballot_number']
        sender_id = message['sender_id']
        last_committed_block = message['last_committed_block']

        if ballot_number > self.promised_number:
            self.promised_number = ballot_number

             # Catch-up mechanism: If the last committed block of the sender is ahead of this server
            if last_committed_block[0] > self.last_committed_block[0]:
                print(f"Server {self.server_id}: Behind, requesting missing blocks from leader.")
                self.request_missing_blocks(sender_id, last_committed_block)
            
            response = {
                'paxos': {
                    'type': 'promise',
                    'sender_id': self.server_id,
                    'ballot_number': ballot_number,
                    'accepted_value': self.accepted_value,
                    'accepted_number': self.accepted_number,
                    'local_transactions':self.transactions_log,
                }
            }

            sender_id_port = next((item for item in self.peers if item % 1000 == sender_id), None)
            self.send_message(sender_id_port, response)

    def handle_promise(self, message):
        ballot_number = message['ballot_number']
        accepted_value = message['accepted_value']
        accepted_number = message['accepted_number']
        local_transactions = message['local_transactions']

        if ballot_number == self.ballot_number:
            self.majority_responses += 1
            if accepted_value is not None and accepted_number > self.ballot_number:
                # Update with accepted value from other servers
                self.local_major_block = accepted_value
            elif local_transactions is not None:
                self.local_major_block += local_transactions
            if self.majority_responses >= MAJORITY:
                # Majority reached, send ACCEPT message
                self.local_major_block += self.transactions_log
                self.send_accept()

    def send_accept(self):
        for peer_port in self.peers:
            message = {'paxos': {
                'type': 'accept',
                'ballot_number': self.ballot_number,
                'sender_id': self.server_id,
                'major_block': self.local_major_block
            }}
            self.send_message(peer_port, message)

    def handle_accept(self, message):
        ballot_number = message['ballot_number']
        major_block = message['major_block']
        sender_id = message['sender_id']

        if ballot_number >= self.promised_number:
            self.accepted_value = major_block
            self.accepted_number = ballot_number
            # Send ACCEPTED message to leader
            response = {'paxos': {
                'type': 'accepted',
                'ballot_number': ballot_number,
                'sender_id': self.server_id,
                'major_block': major_block
            }}
            sender_id_port = next((item for item in self.peers if item % 1000 == sender_id), None)
            self.send_message(sender_id_port, response)

    def handle_accepted(self, message):
        ballot_number = message['ballot_number']
        major_block = message['major_block']

        # Only count accepted responses for the current ballot
        if ballot_number == self.ballot_number:
            self.majority_responses += 1
            print(f"Server {self.server_id}: Received ACCEPTED message from server {message['sender_id']}")

            if self.majority_responses >= MAJORITY:
                # print(f"Server {self.server_id}: Reached majority, committing block {major_block}")
                self.commit_transaction(major_block)

    def commit_transaction(self, major_block):
        # Commit the block locally
        lcm_ballot = (self.ballot_number, self.server_id)
        if (self.last_committed_block[0] >= lcm_ballot[0]):
            return
        self.datastore.append(major_block)
        self.last_committed_block = lcm_ballot
        self.transactions_log.clear()  # Clear the local log as it's now committed
        print(f"Server {self.server_id}: Committed major block {major_block} to datastore.")

        # Broadcast COMMIT message to all other servers
        for peer_port in self.peers:
            message = {
                'paxos': {
                    'type': 'commit',
                    'ballot_number': self.ballot_number,
                    'major_block': major_block,
                    'last_committed_block': lcm_ballot
                }
            }
            self.send_message(peer_port, message)

        self.handle_consensus_completion()

    def handle_commit(self, message):
        # Commit the major block to the datastore
        major_block = message['major_block']
        lcm_ballot = message['last_committed_block']
        print(f"Server {self.server_id}: Committing {major_block} to datastore.")
        self.datastore.append(major_block)
        self.last_committed_block = lcm_ballot
        self.transactions_log.clear()  # Clear the log as it's committed

    def request_missing_blocks(self, leader_id, missing_from_block):
        """Request missing blocks from the leader to catch up."""
        request_message = {
            'paxos': {
                'type': 'catch_up_request',
                'sender_id': self.server_id,
                'missing_from_block': missing_from_block
            }
        }
        leader_port = self.peers[leader_id - 1]
        self.send_message(leader_port, request_message)

    def handle_catch_up_request(self, message):
        missing_from_block = message['missing_from_block']
        requester_id = message['sender_id']

        # Find the missing blocks and send them to the requester
        missing_blocks = self.get_missing_blocks(missing_from_block)
        response_message = {
            'paxos': {
                'type': 'catch_up_response',
                'sender_id': self.server_id,
                'missing_blocks': missing_blocks
            }
        }
        requester_port = self.peers[requester_id - 1]
        self.send_message(requester_port, response_message)

    def handle_catch_up_response(self, message):
        # Append missing blocks to the datastore
        missing_blocks = message['missing_blocks']
        self.datastore.extend(missing_blocks)
        print(f"Server {self.server_id}: Caught up with missing blocks.")


    # -------- Helper Methods -------- #

    def calculate_balance(self):
        self.balance = INITIAL_BALANCE

        all_transactions = []
        for block in self.datastore:
            for transaction in block:
                all_transactions.append(transaction)
        sorted_transactions = sorted(all_transactions, key=lambda t: t[0])

        for transaction in sorted_transactions:
            sender, receiver, amount = transaction[1]
            if sender == self.server_id:
                self.balance -= amount
            elif receiver == self.server_id:
                self.balance += amount

    def handle_consensus_completion(self):
        self.calculate_balance()
    
        if self.pending_transaction:
            self.handle_transaction(self.pending_transaction)
        self.pending_transaction = None
    
    # def check_balance(self):
    #     # Check balance based on local log and committed datastore
    #     balance = self.balance
    #     for transaction in self.transactions_log:
    #         balance -= transaction['amount']
    #     return balance

    def send_message(self, peer_port, message):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            peer_socket.connect(('localhost', peer_port))
            peer_socket.send(json.dumps(message).encode())
        finally:
            peer_socket.close()

            
def send_transaction_to_server(server_port, transaction):
    message = {'transaction': transaction}
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        peer_socket.connect(('localhost', server_port))
        peer_socket.send(json.dumps(message).encode())
    except ConnectionRefusedError:
        print(f"Error: Could not connect to server on port {server_port}. Is the server running?")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        peer_socket.close()

def read_input_file(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        test_sets = {}
        current_set = None
        sequence_number = 0
        for row in reader:
            if row[0]:  # New set
                current_set = int(row[0])
                if current_set not in test_sets:
                    test_sets[current_set] = {'transactions': [], 'live_servers': eval(row[2])}
            transaction = eval(row[1])
            sequence_number += 1 
            test_sets[current_set]['transactions'].append((sequence_number, transaction))
    return test_sets

# Initialize servers and peers
servers = []
ports = [8001, 8002, 8003]
for i in range(len(ports)):
    peers = [port for port in ports if port != ports[i]]
    server = PaxosServer(i + 1, ports[i], peers)
    servers.append(server)
    try:
        threading.Thread(target=server.start_server, daemon=True).start()
    except Exception as e:
        print(f"Error starting server thread: {e}")

time.sleep(2)
test_sets = read_input_file('tests/input.csv')
for set_number, test_data in test_sets.items():
    print(f"Running Test Set {set_number}...")
    transactions = test_data['transactions']
    live_servers = test_data['live_servers']
    
    for transaction in transactions:
        sender_server_id = transaction[1][0]  # S is the sender, which determines the server
        server_port = ports[sender_server_id - 1]
        
        # If the server is in the live_servers, send the transaction to that server
        if server_port%1000 in live_servers:
            print(f"Sending transaction {transaction} to server {sender_server_id} on port {server_port}")
            time.sleep(1)
            send_transaction_to_server(server_port, transaction)
        else:
            print(f"Server {sender_server_id} is down, skipping transaction {transaction}")
    input(f"Test Set {set_number} executed. Press Enter to continue to the next set...")
