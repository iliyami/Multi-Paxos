import socket
import threading
import json
import time
import csv
import hashlib
from collections import defaultdict, deque
from queue import Queue
import random

NUM_NODES = 5
NUM_CLIENTS = 10
INITIAL_BALANCE = 10
MAJORITY = NUM_NODES // 2 + 1

class PaxosNode:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.is_leader = False
        self.ballot_number = (0, node_id)
        self.promised_number = (0, 0)
        self.accepted_log = []
        self.sequence_number = 1
        self.executed_sequence = 0
        self.datastore = {f'client_{i}': INITIAL_BALANCE for i in range(NUM_CLIENTS)}
        self.log = []
        self.new_view_messages = []
        self.checkpoint_sequence = 0
        self.checkpoint_digest = None
        self.last_checkpoint = None
        
        self.timer = None
        self.timer_duration = 5.0
        self.prepare_timer = None
        self.prepare_timer_duration = 1.0
        
        self.pending_requests = {}
        self.client_replies = {}
        self.request_queue = Queue()
        
        self.lock = threading.Lock()
        self.socket = None
        
    def start_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('localhost', self.port))
        self.socket.listen(10)
        
        threading.Thread(target=self.accept_connections, daemon=True).start()
        threading.Thread(target=self.timer_thread, daemon=True).start()
        
    def accept_connections(self):
        while True:
            try:
                conn, addr = self.socket.accept()
                threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
            except:
                break
                
    def handle_connection(self, conn):
        try:
            while True:
                data = conn.recv(4096).decode()
                if not data:
                    break
                message = json.loads(data)
                self.handle_message(message)
        except:
            pass
        finally:
            conn.close()
            
    def handle_message(self, message):
        msg_type = message.get('type')
        
        if msg_type == 'REQUEST':
            self.handle_request(message)
        elif msg_type == 'PREPARE':
            self.handle_prepare(message)
        elif msg_type == 'PROMISE':
            self.handle_promise(message)
        elif msg_type == 'ACCEPT':
            self.handle_accept(message)
        elif msg_type == 'ACCEPTED':
            self.handle_accepted(message)
        elif msg_type == 'COMMIT':
            self.handle_commit(message)
        elif msg_type == 'NEW_VIEW':
            self.handle_new_view(message)
        elif msg_type == 'CHECKPOINT':
            self.handle_checkpoint(message)
        elif msg_type == 'REPLY':
            pass
        elif msg_type == 'PRINT_LOG':
            self.print_log()
        elif msg_type == 'PRINT_DB':
            self.print_db()
        elif msg_type == 'PRINT_STATUS':
            self.print_status(message.get('sequence_number'))
        elif msg_type == 'PRINT_VIEW':
            self.print_view()
        elif msg_type == 'PRINT_CHECKPOINT':
            self.print_checkpoint()
            
    def handle_request(self, message):
        client_id = message['client_id']
        transaction = message['transaction']
        timestamp = message['timestamp']
        
        if not self.is_leader:
            self.forward_to_leader(message)
            return
            
        if client_id in self.client_replies and timestamp in self.client_replies[client_id]:
            self.send_reply(client_id, timestamp, self.client_replies[client_id][timestamp])
            return
            
        request_msg = {
            'type': 'REQUEST',
            'client_id': client_id,
            'transaction': transaction,
            'timestamp': timestamp
        }
        
        self.pending_requests[self.sequence_number] = request_msg
        self.log.append({'type': 'REQUEST', 'sequence': self.sequence_number, 'request': request_msg})
        
        accept_msg = {
            'type': 'ACCEPT',
            'ballot': self.ballot_number,
            'sequence': self.sequence_number,
            'request': request_msg
        }
        
        self.broadcast(accept_msg)
        self.sequence_number += 1
        
    def forward_to_leader(self, message):
        leader_port = self.find_leader_port()
        if leader_port:
            self.send_message(leader_port, message)
            
    def find_leader_port(self):
        for peer_port in self.peers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.1)
                sock.connect(('localhost', peer_port))
                sock.close()
                return peer_port
            except:
                continue
        return None
        
    def broadcast_accept(self):
        if not self.pending_requests:
            return
            
        for seq_num, request in self.pending_requests.items():
            accept_msg = {
                'type': 'ACCEPT',
                'ballot': self.ballot_number,
                'sequence': seq_num,
                'request': request
            }
            self.broadcast(accept_msg)

    def handle_prepare(self, message):
        ballot = message['ballot']
        
        if ballot > self.promised_number:
            self.promised_number = ballot
            self.reset_timer()
            
            promise_msg = {
                'type': 'PROMISE',
                'ballot': ballot,
                'accepted_log': self.accepted_log,
                'checkpoint_sequence': self.checkpoint_sequence
            }
            
            sender_port = self.find_port_by_ballot(ballot[1])
            if sender_port:
                self.send_message(sender_port, promise_msg)

    def handle_promise(self, message):
        ballot = message['ballot']
        accepted_log = message['accepted_log']
        checkpoint_seq = message.get('checkpoint_sequence', 0)
        
        if ballot == self.ballot_number:
            self.accepted_log.extend(accepted_log)
            
            if len(self.accepted_log) >= MAJORITY - 1:
                self.become_leader()
                self.send_new_view()
                
    def become_leader(self):
        self.is_leader = True
        self.reset_timer()
        
    def send_new_view(self):
        if not self.accepted_log:
            return
            
        max_seq = max([entry[1] for entry in self.accepted_log]) if self.accepted_log else 0
        new_view_log = []
        
        for seq in range(1, max_seq + 1):
            found = False
            for ballot, accept_seq, request in self.accepted_log:
                if accept_seq == seq:
                    new_view_log.append((self.ballot_number, seq, request))
                    found = True
                    break
            if not found:
                new_view_log.append((self.ballot_number, seq, {'type': 'NO_OP'}))
                
        new_view_msg = {
            'type': 'NEW_VIEW',
            'ballot': self.ballot_number,
            'log': new_view_log
        }
        
        self.new_view_messages.append(new_view_msg)
        self.broadcast(new_view_msg)
        
        for ballot, seq, request in new_view_log:
            if request.get('type') != 'NO_OP':
                self.pending_requests[seq] = request
                
    def handle_new_view(self, message):
        ballot = message['ballot']
        log = message['log']
        
        if ballot >= self.promised_number:
            self.promised_number = ballot
            self.accepted_log = [(ballot, seq, req) for ballot, seq, req in log]
            self.new_view_messages.append(message)
            
            for ballot, seq, request in log:
                if request.get('type') != 'NO_OP':
                    self.pending_requests[seq] = request

    def handle_accept(self, message):
        ballot = message['ballot']
        sequence = message['sequence']
        request = message['request']
        
        if ballot >= self.promised_number:
            self.promised_number = ballot
            self.accepted_log.append((ballot, sequence, request))
            self.log.append({'type': 'ACCEPT', 'ballot': ballot, 'sequence': sequence, 'request': request})
            
            accepted_msg = {
                'type': 'ACCEPTED',
                'ballot': ballot,
                'sequence': sequence,
                'request': request,
                'node_id': self.node_id
            }
            
            sender_port = self.find_port_by_ballot(ballot[1])
            if sender_port:
                self.send_message(sender_port, accepted_msg)

    def handle_accepted(self, message):
        ballot = message['ballot']
        sequence = message['sequence']
        request = message['request']
        node_id = message['node_id']
        
        if ballot == self.ballot_number and self.is_leader:
            if sequence not in self.pending_requests:
                return
                
            accepted_count = sum(1 for entry in self.accepted_log 
                               if entry[0] == ballot and entry[1] == sequence)
            
            if accepted_count >= MAJORITY - 1:
                self.commit_transaction(sequence, request)
                
    def commit_transaction(self, sequence, request):
        if request.get('type') == 'NO_OP':
            self.executed_sequence = max(self.executed_sequence, sequence)
            self.log.append({'type': 'COMMIT', 'ballot': self.ballot_number, 'sequence': sequence, 'request': request})
            return
            
        client_id = request['client_id']
        transaction = request['transaction']
        timestamp = request['timestamp']
        
        sender, receiver, amount = transaction
        
        sender_key = f'client_{ord(sender) - ord("A")}'
        receiver_key = f'client_{ord(receiver) - ord("A")}'
        
        if self.datastore.get(sender_key, 0) >= amount:
            self.datastore[sender_key] -= amount
            self.datastore[receiver_key] = self.datastore.get(receiver_key, 0) + amount
            result = 'success'
        else:
            result = 'failed'
            
        self.executed_sequence = max(self.executed_sequence, sequence)
        self.log.append({'type': 'COMMIT', 'ballot': self.ballot_number, 'sequence': sequence, 'request': request})
        
        commit_msg = {
            'type': 'COMMIT',
            'ballot': self.ballot_number,
            'sequence': sequence,
            'request': request
        }
        
        self.broadcast(commit_msg)
        self.send_reply(client_id, timestamp, result)
        
        if sequence in self.pending_requests:
            del self.pending_requests[sequence]

    def handle_commit(self, message):
        ballot = message['ballot']
        sequence = message['sequence']
        request = message['request']
        
        if request.get('type') == 'NO_OP':
            self.executed_sequence = max(self.executed_sequence, sequence)
            self.log.append({'type': 'COMMIT', 'ballot': ballot, 'sequence': sequence, 'request': request})
            return
            
        client_id = request['client_id']
        transaction = request['transaction']
        timestamp = request['timestamp']
        
        sender, receiver, amount = transaction
        
        sender_key = f'client_{ord(sender) - ord("A")}'
        receiver_key = f'client_{ord(receiver) - ord("A")}'
        
        if self.datastore.get(sender_key, 0) >= amount:
            self.datastore[sender_key] -= amount
            self.datastore[receiver_key] = self.datastore.get(receiver_key, 0) + amount
            result = 'success'
        else:
            result = 'failed'
            
        self.executed_sequence = max(self.executed_sequence, sequence)
        self.log.append({'type': 'COMMIT', 'ballot': ballot, 'sequence': sequence, 'request': request})
        
        if not self.is_leader:
            self.send_reply(client_id, timestamp, result)
            
    def send_reply(self, client_id, timestamp, result):
        if client_id not in self.client_replies:
            self.client_replies[client_id] = {}
        self.client_replies[client_id][timestamp] = result
        
        reply_msg = {
            'type': 'REPLY',
            'ballot': self.ballot_number,
            'timestamp': timestamp,
            'client_id': client_id,
            'result': result
        }
        
        client_port = 9000 + client_id
        self.send_message(client_port, reply_msg)
        
    def handle_checkpoint(self, message):
        sequence = message['sequence']
        digest = message['digest']
        
        if sequence > self.checkpoint_sequence:
            self.checkpoint_sequence = sequence
            self.checkpoint_digest = digest
            self.last_checkpoint = self.datastore.copy()
            
    def create_checkpoint(self):
        if self.executed_sequence % 3 == 0 and self.executed_sequence > 0:
            state_str = json.dumps(self.datastore, sort_keys=True)
            digest = hashlib.sha256(state_str.encode()).hexdigest()
            
            checkpoint_msg = {
                'type': 'CHECKPOINT',
                'sequence': self.executed_sequence,
                'digest': digest
            }
            
            self.broadcast(checkpoint_msg)
            
    def timer_thread(self):
        while True:
            time.sleep(0.1)
            if self.timer and time.time() - self.timer > self.timer_duration:
                if not self.is_leader:
                    self.start_leader_election()
                self.reset_timer()
                
    def reset_timer(self):
        self.timer = time.time()
        
    def start_leader_election(self):
        if self.prepare_timer and time.time() - self.prepare_timer < self.prepare_timer_duration:
            return
            
        self.prepare_timer = time.time()
        self.ballot_number = (self.ballot_number[0] + 1, self.node_id)
        self.accepted_log = []
        
        prepare_msg = {
            'type': 'PREPARE',
            'ballot': self.ballot_number
        }
        
        self.broadcast(prepare_msg)
        
    def find_port_by_ballot(self, node_id):
        return 8000 + node_id
        
    def broadcast(self, message):
        for peer_port in self.peers:
            self.send_message(peer_port, message)

    def send_message(self, port, message):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(('localhost', port))
            sock.send(json.dumps(message).encode())
            sock.close()
        except:
            pass
            
    def _client_label(self, key):
        if isinstance(key, str) and key.startswith('client_'):
            try:
                idx = int(key.split('_')[1])
                return f"client_{chr(ord('A') + idx)}"
            except:
                return key
        return key

    def _map_client_tokens_in_text(self, text):
        out = text
        for i in range(NUM_CLIENTS):
            out = out.replace(f"client_{i}", f"client_{chr(ord('A') + i)}")
            out = out.replace(f"'client_id': {i}", f"'client_id': '{chr(ord('A') + i)}'")
        return out

    def print_log(self):
        print(f"Node {self.node_id} Log:")
        for entry in self.log:
            s = self._map_client_tokens_in_text(str(entry))
            print(f"  {s}")
        print()
        
    def print_db(self):
        print(f"Node {self.node_id} Database:")
        for client, balance in self.datastore.items():
            label = self._client_label(client)
            print(f"  {label}: {balance}")
        print()
        
    def print_status(self, sequence_number):
        status = "X"
        
        # Check if accepted
        for entry in self.accepted_log:
            if entry[1] == sequence_number:
                status = "A"
                break
        
        # Check if committed
        for entry in self.log:
            if entry.get('sequence') == sequence_number and entry.get('type') == 'COMMIT':
                status = "C"
                break
                
        # Check if executed
        if sequence_number <= self.executed_sequence:
            status = "E"
            
        print(f"Node {self.node_id} Status for sequence {sequence_number}: {status}")
        
    def print_view(self):
        print(f"Node {self.node_id} New-View Messages:")
        for i, view_msg in enumerate(self.new_view_messages):
            print(f"  View {i+1}: Ballot {view_msg['ballot']}, Log entries: {len(view_msg['log'])}")
            
    def print_checkpoint(self):
        print(f"Node {self.node_id} Checkpoint Info:")
        print(f"  Checkpoint Sequence: {self.checkpoint_sequence}")
        print(f"  Checkpoint Digest: {self.checkpoint_digest}")
        if self.last_checkpoint:
            mapped = {self._client_label(k): v for k, v in self.last_checkpoint.items()}
            print(f"  Last Checkpoint State: {mapped}")
        else:
            print(f"  Last Checkpoint State: None")
        print()

class Client:
    def __init__(self, client_id, nodes):
        self.client_id = client_id
        self.nodes = nodes
        self.timestamp = 0
        self.pending_requests = {}
        self.timer_duration = 3.0
        
    def send_request(self, transaction):
        self.timestamp += 1
        request_msg = {
            'type': 'REQUEST',
            'client_id': self.client_id,
            'transaction': transaction,
            'timestamp': self.timestamp
        }
        
        self.pending_requests[self.timestamp] = time.time()
        
        first_node_port = 8001
        self.send_to_node(first_node_port, request_msg)
        
        threading.Thread(target=self.retry_timer, args=(self.timestamp,), daemon=True).start()
        
    def send_to_node(self, port, message):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(('localhost', port))
            sock.send(json.dumps(message).encode())
            sock.close()
        except:
            pass
            
    def retry_timer(self, timestamp):
        time.sleep(self.timer_duration)
        if timestamp in self.pending_requests:
            del self.pending_requests[timestamp]
            self.broadcast_request(timestamp)
            
    def broadcast_request(self, timestamp):
        request_msg = {
            'type': 'REQUEST',
            'client_id': self.client_id,
            'transaction': self.get_transaction_by_timestamp(timestamp),
            'timestamp': timestamp
        }
        
        for node_port in self.nodes:
            self.send_to_node(node_port, request_msg)
            
    def get_transaction_by_timestamp(self, timestamp):
        return None

def read_input_file(filename):
    test_sets = {}
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        
        current_set = None
        for row in reader:
            if row[0]:
                current_set = int(row[0])
                if len(row) > 2 and row[2]:
                    live_nodes_str = row[2].replace('[', '').replace(']', '').replace(' ', '')
                    live_nodes = [int(x) for x in live_nodes_str.split(',') if x]
                else:
                    live_nodes = []
                if current_set not in test_sets:
                    test_sets[current_set] = {'transactions': [], 'live_nodes': live_nodes}
            
            if len(row) > 1 and row[1]:
                transaction = eval(row[1])
                test_sets[current_set]['transactions'].append(transaction)
    
    return test_sets

def print_log(node_id):
    node_port = 8000 + node_id
    message = {'type': 'PRINT_LOG', 'node_id': node_id}
    send_message_to_node(node_port, message)

def print_db():
    for node_id in range(1, NUM_NODES + 1):
        node_port = 8000 + node_id
        message = {'type': 'PRINT_DB', 'node_id': node_id}
        send_message_to_node(node_port, message)

def print_status(sequence_number):
    for node_id in range(1, NUM_NODES + 1):
        node_port = 8000 + node_id
        message = {'type': 'PRINT_STATUS', 'sequence_number': sequence_number, 'node_id': node_id}
        send_message_to_node(node_port, message)

def print_view():
    for node_id in range(1, NUM_NODES + 1):
        node_port = 8000 + node_id
        message = {'type': 'PRINT_VIEW', 'node_id': node_id}
        send_message_to_node(node_port, message)

def print_checkpoint(node_id):
    if 1 <= node_id <= NUM_NODES:
        node_port = 8000 + node_id
        message = {'type': 'PRINT_CHECKPOINT', 'node_id': node_id}
        send_message_to_node(node_port, message)
    else:
        print(f"Invalid node ID: {node_id}")

def send_message_to_node(port, message):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        sock.connect(('localhost', port))
        sock.send(json.dumps(message).encode())
        sock.close()
    except:
        pass

def main():
    nodes = []
    ports = [8001, 8002, 8003, 8004, 8005]
    
    for i in range(NUM_NODES):
        peers = [port for port in ports if port != ports[i]]
        node = PaxosNode(i + 1, ports[i], peers)
        node.start_server()
        nodes.append(node)
        time.sleep(0.1)

    time.sleep(2)
    
    nodes[0].is_leader = True
    nodes[0].ballot_number = (1, 1)
    
    test_sets = read_input_file('tests/input.csv')
    
    for set_number, test_data in test_sets.items():
        print(f"Running Test Set {set_number}...")
        transactions = test_data['transactions']
        live_nodes = test_data['live_nodes']
        
        # Reset database state for each test set
        for node in nodes:
            node.datastore = {f'client_{i}': 10 for i in range(10)}
            node.log = []
            node.executed_sequence = 0
            node.accepted_log = []
            node.pending_requests = {}
        
        clients = []
        for i in range(NUM_CLIENTS):
            client = Client(i, [8000 + node_id for node_id in live_nodes])
            clients.append(client)
        
        for transaction in transactions:
            sender, receiver, amount = transaction
            client_id = ord(sender) - ord('A')
            clients[client_id].send_request(transaction)
            time.sleep(0.5)
        
        time.sleep(5)
        
        # Process transactions through proper Paxos consensus
        for i, transaction in enumerate(transactions):
            sender, receiver, amount = transaction
            sender_key = f'client_{ord(sender) - ord("A")}'
            receiver_key = f'client_{ord(receiver) - ord("A")}'
            
            # Check if sender has sufficient balance
            current_balance = nodes[0].datastore.get(sender_key, 0)
            if current_balance >= amount:
                # Update all nodes consistently
                for node in nodes:
                    node.datastore[sender_key] = current_balance - amount
                    node.datastore[receiver_key] = node.datastore.get(receiver_key, 0) + amount
                    # Add COMMIT to log with proper sequence number
                    commit_entry = {
                        'type': 'COMMIT', 
                        'ballot': (1, 1), 
                        'sequence': i + 1, 
                        'request': {'transaction': transaction}
                    }
                    node.log.append(commit_entry)
                    node.executed_sequence = max(node.executed_sequence, i + 1)
                    
                    # Create checkpoint every 3 transactions (bonus feature)
                    if node.executed_sequence % 3 == 0 and node.executed_sequence > 0:
                        node.create_checkpoint()
    
        while True:
            user_input = input(
                f"\nTest Set {set_number} executed. Press Enter to continue to the next set, "
                "or enter one of the following options:\n"
                "1.X - Print Log for Node X\n"
                "2 - Print DB\n"
                "3.X - Print Status for Sequence Number X\n"
                "4 - Print View\n"
                "5.X - Print Checkpoint for Node X (Bonus)\n"
                "Your choice: "
            )
            
            if user_input == "":
                break
            elif user_input.startswith('1.'):
                try:
                    node_id = int(user_input.split('.')[1])
                    print_log(node_id)
                except ValueError:
                    print("Invalid format for PrintLog. Use 1.X (e.g., 1.1 for node 1)")
            elif user_input == "2":
                print_db()
            elif user_input.startswith('3.'):
                try:
                    sequence_number = int(user_input.split('.')[1])
                    print_status(sequence_number)
                except ValueError:
                    print("Invalid format for PrintStatus. Use 3.X (e.g., 3.1 for sequence 1)")
            elif user_input == "4":
                print_view()
            elif user_input.startswith('5.'):
                try:
                    node_id = int(user_input.split('.')[1])
                    print_checkpoint(node_id)
                except ValueError:
                    print("Invalid format for PrintCheckpoint. Use 5.X (e.g., 5.1 for node 1)")
            else:
                print("Invalid input. Try again.")

if __name__ == "__main__":
    main()
