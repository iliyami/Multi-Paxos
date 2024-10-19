import socket
import threading
import json
from queue import Queue
import time
import csv
import sqlite3
import os

# Constants
INITIAL_BALANCE = 100
NUM_SERVERS = 3
MAJORITY = NUM_SERVERS // 2 + 1  # Paxos requires a majority to commit


# Sample server class handling TCP connections and Paxos protocol
class PaxosServer:
    round_number = 0
    pending_paxos = False
    def __init__(self, server_id, port, peers, db_file):
        self.server_id = server_id
        self.port = port
        self.peers = peers  # List of peer server ports
        self.transactions_log = []  # Local log of transactions
        self.balance = INITIAL_BALANCE  # Initial balance for this server
        self.is_leader = False
        self.ballot_number = 0
        self.promised_number = 0  # For promise phase
        self.accepted_value = None
        self.accepted_number = 0
        self.majority_responses = 0
        self.majority_reached = False
        self.local_major_block = []
        self.lock = threading.Lock()  # For thread safety
        self.last_committed_block = (0, 0)
        self.transaction_queue = Queue()

        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS transactions
                               (sequence_number INTEGER PRIMARY KEY,
                                sender INTEGER,
                                receiver INTEGER,
                                amount INTEGER,
                                ballot_number INTEGER,
                                process_id INTEGER)''')
        self.conn.commit()

    def close(self):
        self.conn.close()

    def add_transaction_to_datastore(self, block, ballot):
        ballot_number, process_id = ballot
        new_curstor = self.conn.cursor()
        data_to_insert = []
        for transaction in block:
            sequence_number, details = transaction[0], transaction[1]
            sender, receiver, amount = details
            data_to_insert.append((sequence_number, sender, receiver, amount, ballot_number, process_id))

        new_curstor.executemany('''
            INSERT OR IGNORE INTO transactions (sequence_number, sender, receiver, amount, ballot_number, process_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', data_to_insert)
            
        if new_curstor.rowcount > 0:
            self.conn.commit()
        new_curstor.close()
        print(f"Server {self.server_id}: Transaction {block} added to persistent datastore (DB).")

    def replace_datastore(self, new_datastore):
        self.cursor.execute('DELETE FROM transactions')
        self.conn.commit()

        for transaction in new_datastore:
            sequence_number, sender, receiver, amount, ballot_number, process_id = transaction

            self.cursor.execute('''
                INSERT OR IGNORE INTO transactions (sequence_number, sender, receiver, amount, ballot_number, process_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (sequence_number, sender, receiver, amount, ballot_number, process_id))

        self.conn.commit()
        print(f"Server {self.server_id}: Replaced the datastore with the new given datastore.")

    def get_all_transactions(self):
        new_cursor = self.conn.cursor()
        new_cursor.execute('SELECT * FROM transactions')
        transactions = new_cursor.fetchall()
        return transactions
        
    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        elif'command' in request:
            self.handle_command(request['command'])

    # ------------- Paxos Phases ----------------- #
    def handle_command(self, command):
        command_type = command['type']
        if command_type == 'client_balance_request':
            self.client_balance_request(command)
        elif command_type == 'client_balance_response':
            self.client_balance_response(command)
        elif command_type == 'print_balance':
            self.client_balance(command)
        elif command_type == 'print_log':
            self.local_logs()
        elif command_type == 'print_db':
            self.db_dump()
        elif command_type == 'performance':
            self.performance()
    
    def handle_transaction(self, transaction):
        if PaxosServer.pending_paxos:
            print(f"Server {self.server_id}: Paxos is in progress. Queuing transaction {transaction}.")
            self.transaction_queue.put(transaction)
            return
        seq_num, trans = transaction
        sender, receiver, amount = trans
        # If balance is insufficient, initiate Paxos protocol
        self.check_balance()
        if self.balance < amount:
            print(f"Server {self.server_id}: Queuing transaction {transaction}.")
            self.transaction_queue.put(transaction)
            self.initiate_paxos(transaction)
        else:
            # Process the transaction locally and update log
            self.balance -= amount
            self.transactions_log.append(transaction)
            print(f"Server {self.server_id}: Processed transaction {transaction}")

    def initiate_paxos(self, transaction):
        PaxosServer.pending_paxos = True
        PaxosServer.round_number += 1
        self.ballot_number = PaxosServer.round_number
        # Send PREPARE message to all peers
        for peer_port in self.peers:
            self.send_prepare(peer_port)

    def send_prepare(self, peer_port):
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
        leader_lcm = message['last_committed_block']

        if leader_lcm[0] >= self.last_committed_block[0] and ballot_number > self.promised_number:
            self.promised_number = ballot_number

             # Catch-up mechanism: If the last committed block of the sender is ahead of this server
            if leader_lcm[0] > self.last_committed_block[0]:
                print(f"Server {self.server_id}: Behind, requesting missing blocks from leader.")
                self.request_missing_blocks(sender_id, leader_lcm, self.last_committed_block)
            
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

            sender_id_port = self.find_port(sender_id)
            # print(f'Sending Promissssse because of: {transaction} ballot: {ballot_number}, last block msg: {last_committed_block} last block: {self.last_committed_block}')
            self.send_message(sender_id_port, response)

    def handle_promise(self, message):
        ballot_number = message['ballot_number']
        accepted_value = message['accepted_value']
        accepted_number = message['accepted_number']
        local_transactions = message['local_transactions']

        isUpToDate = True
        if ballot_number == self.ballot_number:
            self.majority_responses += 1
            if accepted_value is not None and accepted_number > self.ballot_number:
                # Update with accepted value from other servers
                self.local_major_block = accepted_value
                isUpToDate = False
            elif local_transactions is not None:
                self.local_major_block += local_transactions
            if self.majority_responses >= MAJORITY:
                # Majority reached, send ACCEPT message
                if isUpToDate:
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

            if self.majority_responses >= MAJORITY and self.majority_reached == False:
                self.majority_reached = True
                # print(f"Server {self.server_id}: Reached majority, committing block {major_block}")
                self.commit_transaction(major_block)

    def commit_transaction(self, major_block):
        # Commit the block locally
        lcm_ballot = (self.ballot_number, self.server_id)
        if (self.last_committed_block[0] >= lcm_ballot[0]):
            return
        unique_major_block = []
        for trans in major_block:
            if trans not in unique_major_block:
                unique_major_block.append(trans)
        self.add_transaction_to_datastore(unique_major_block, lcm_ballot)
        self.last_committed_block = lcm_ballot
        self.majority_reached = False
        self.clear_outdated_logs(unique_major_block)
        PaxosServer.pending_paxos = False

        # Broadcast COMMIT message to all other servers
        for peer_port in self.peers:
            message = {
                'paxos': {
                    'type': 'commit',
                    'ballot_number': self.ballot_number,
                    'major_block': unique_major_block,
                    'last_committed_block': lcm_ballot
                }
            }
            self.send_message(peer_port, message)

        self.handle_consensus_completion()

    def handle_commit(self, message):
        # Commit the major block to the datastore
        major_block = message['major_block']
        lcm_ballot = message['last_committed_block']
        self.add_transaction_to_datastore(major_block, lcm_ballot)
        self.last_committed_block = lcm_ballot
        self.clear_outdated_logs(major_block)  # Clear the log as it's committed
        self.handle_consensus_completion()

    def clear_outdated_logs(self, major_block):
        mb_sequences = [item[0] for item in major_block]
        self.transactions_log = [transaction for transaction in self.transactions_log if transaction[0] not in mb_sequences]
        self.accepted_number = 0
        self.accepted_value = None

    def request_missing_blocks(self, leader_id, last_committed_block, requester_lcb):
        """Request missing blocks from the leader to catch up."""
        request_message = {
            'paxos': {
                'type': 'catch_up_request',
                'sender_id': self.server_id,
                'last_committed_block': last_committed_block,
            }
        }
        leader_port = self.find_port(leader_id)
        self.send_message(leader_port, request_message)

    def handle_catch_up_request(self, message):
        last_committed_block = message['last_committed_block']
        requester_id = message['sender_id']

        # Find the missing blocks and send them to the requester
        missing_blocks = self.get_missing_blocks()
        response_message = {
            'paxos': {
                'type': 'catch_up_response',
                'sender_id': self.server_id,
                'missing_blocks': missing_blocks,
                'last_committed_block': last_committed_block
            }
        }
        requester_port = self.find_port(requester_id)
        self.send_message(requester_port, response_message)

    def handle_catch_up_response(self, message):
        # Append missing blocks to the datastore
        missing_blocks = message['missing_blocks']
        lcm_ballot = message['last_committed_block']
        self.replace_datastore(missing_blocks)
        self.last_committed_block = lcm_ballot
        self.clear_outdated_logs(missing_blocks)  # Clear the local log as it's now committed
        print(f"Server {self.server_id}: Caught up with missing blocks.")


    # -------- Commands -------- #
    def client_balance(self, command):
        client = command['client']
        if (client == None):
            print('Wrong request format!')
        balance = self.calculate_balance(client)
        print(f'Client {client} balance on server {self.server_id} is {balance}')

    def local_logs(self):
        print(f'Server {self.server_id} local logs:\n{self.transactions_log}')

    def db_dump(self):
        print(f'Server {self.server_id} datastore dump:\n{self.get_all_transactions()}')

    def performance(self):
        print(f'Server {self.server_id} performance is -')

    def client_balance_request(self, command):
        client = command['client']
        message = {
            'command': {
                'type': 'client_balance_response',
                'sender_id': self.server_id,
                'client': client,
            }
        }
        for peer_port in self.peers:
            self.send_message(peer_port, message)
        print(f'Client {client} total balance is {self.calculate_balance(client)} in server {self.server_id}')

    def client_balance_response(self, message):
        sender_id = message['sender_id']
        client = message['client']
        balance = self.calculate_balance(client)
        print(f'Client {client} total balance is {balance} in server {self.server_id}')

    # -------- Helper Methods -------- #

    def find_port(self, sender_id):
        return next((item for item in self.peers if item % 1000 == sender_id), None)

    def calculate_balance(self, client=None):
        if client == None:
            client = self.server_id
        balance = INITIAL_BALANCE

        all_transactions = self.transactions_log.copy()
        datastore = self.get_all_transactions()
        for trans in datastore:
            sequence_number, sender, receiver, amount, ballot_number, process_id = trans
            all_transactions.append([sequence_number, [sender, receiver, amount]])
        sorted_transactions = sorted(all_transactions, key=lambda t: t[0])

        for transaction in sorted_transactions:
            sender, receiver, amount = transaction[1]
            if sender == client:
                balance -= amount
            elif receiver == client:
                balance += amount
        if client == self.server_id:
            self.balance = balance
        return balance

    def handle_consensus_completion(self):
        self.calculate_balance()
        self.process_queued_transactions()
    
    def check_balance(self):
        self.calculate_balance()

    def send_message(self, peer_port, message):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            peer_socket.connect(('localhost', peer_port))
            peer_socket.send(json.dumps(message).encode())
        finally:
            peer_socket.close()

    def process_queued_transactions(self):
        while not self.transaction_queue.empty():
            transaction = self.transaction_queue.get()
            print(f"Server {self.server_id}: Processing queued transaction {transaction}.")
            self.handle_transaction(transaction)

    def get_missing_blocks(self):
        return self.get_all_transactions()

            
def send_transaction_to_server(server_port, transaction, live_servers):
    message = {'transaction': transaction, 'live_servers': live_servers}
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

def send_message_to_server(server_port, message):
    try:
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

def start_server(server_id, port, peers):
    db_file = f'dbs/server_{server_id}.db'
    # Remove the existing database file if it exists
    if os.path.exists(db_file):
        os.remove(db_file)
    server = PaxosServer(server_id, port, peers, db_file=db_file)
    server.start_server()
    return server

def print_balance(client, server_id):
    server_port = server_id + 8000
    message = {'command': {
        'type': 'print_balance',
        'client': client,
    }}
    send_message_to_server(server_port, message)

def print_log(server_id):
    server_port = server_id + 8000
    message = {'command': {
        'type': 'print_log',
    }}
    send_message_to_server(server_port, message)

def print_db(server_id):
    server_port = server_id + 8000
    message = {'command': {
        'type': 'print_db',
    }}
    send_message_to_server(server_port, message)

def performance(server_id):
    server_port = server_id + 8000
    message = {'command': {
        'type': 'performance',
    }}
    send_message_to_server(server_port, message)

def print_balance_across_servers(client):
    message = {'command': {
        'type': 'client_balance_request',
        'client': client
    }}
    send_message_to_server(server_port, message)



# Main
threads = []
ports = [8001, 8002, 8003]
for i in range(len(ports)):
    peers = [port for port in ports if port != ports[i]]
    try:
        thread = threading.Thread(target=start_server, args=(i + 1, ports[i], peers), daemon=True)
        thread.start()
        threads.append(thread)
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
            # print(f"Sending transaction {transaction} to server {sender_server_id} on port {server_port}")
            # time.sleep(1)
            send_transaction_to_server(server_port, transaction, live_servers)
        else:
            print(f"Server {sender_server_id} is down, skipping transaction {transaction}")
    
    while True:
        user_input = input(
            f"\nTest Set {set_number} executed. Press Enter to continue to the next set, "
            "or enter one of the following options:\n"
            "1.X.Y - Print Balance for Client X on Server Y\n"
            "2.X - Print Log for Server X\n"
            "3.X - Print DB for Server X\n"
            "4.X - Performance of Server X\n"
            "5.X (Bonus) - Aggregated Client X Balance Across All Servers"
            "Your choice: "
        )
        if user_input == "":
            break  # Move to the next set
        elif user_input.startswith('1.'):
            try:
                _, client, server_id = map(int, user_input.split('.'))
                print_balance(client, server_id)
            except ValueError:
                print("Invalid format for PrintBalance. Use 1.X.Y (e.g., 1.1.2 for client 1 on server 2)")
        elif user_input.startswith('2.'):
            server_id = int(user_input.split('.')[1])
            print_log(server_id)
        elif user_input.startswith('3.'):
            server_id = int(user_input.split('.')[1])
            print_db(server_id)
        elif user_input.startswith('4.'):
            server_id = int(user_input.split('.')[1])
            performance(server_id)
        elif user_input.startswith('5.'):
            client = int(user_input.split('.')[1])
            print_balance_across_servers(client)
        else:
            print("Invalid input. Try again.")
