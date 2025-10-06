import socket
import threading
import json
import time
import csv
import hashlib
import argparse
from collections import defaultdict, deque
from queue import Queue
import random

NUM_NODES = 5
NUM_CLIENTS = 10
INITIAL_BALANCE = 10
MAJORITY = NUM_NODES // 2 + 1

# Global debug flag
DEBUG_MODE = False

def debug_print(message):
    """Print debug message only if debug mode is enabled"""
    if DEBUG_MODE:
        print(message)

class PaxosNode:
    # Shared class variable to track the current leader
    leader_id = 1  # Start with node 1 as leader
    
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
        self.committed_sequences = set()
        
        # Transaction processing queue and lock
        self.transaction_queue = Queue()
        self.processing_lock = threading.Lock()
        self.is_processing = False
        self.is_isolated = False
        self.is_failed = False
        
        self.lock = threading.Lock()
        self.socket = None
        
    def start_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('localhost', self.port))
        self.socket.listen(10)
        
        threading.Thread(target=self.accept_connections, daemon=True).start()
        threading.Thread(target=self.timer_thread, daemon=True).start()
        threading.Thread(target=self.transaction_processor, daemon=True).start()
        
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
        
        # Allow print commands even for isolated nodes
        if msg_type == 'PRINT_LOG':
            self.print_log()
        elif msg_type == 'PRINT_DB':
            self.print_db()
        elif msg_type == 'PRINT_STATUS':
            self.print_status(message.get('sequence_number'))
        elif msg_type == 'PRINT_VIEW':
            self.print_view()
        elif msg_type == 'PRINT_CHECKPOINT':
            self.print_checkpoint()
        elif msg_type == 'PROCESS_QUEUE':
            self.process_transaction_queue()
        # If this node is isolated or failed, ignore all consensus messages
        elif self.is_isolated or self.is_failed:
            return
        elif msg_type == 'REQUEST':
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
        elif msg_type == 'LEADER_FAIL':
            self.handle_leader_failure()
        elif msg_type == 'CATCH_UP_REQUEST':
            self.handle_catch_up_request(message)
        elif msg_type == 'CATCH_UP_RESPONSE':
            self.handle_catch_up_response(message)
            
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
            
        # Add transaction to queue instead of processing immediately
        request_msg = {
            'type': 'REQUEST',
            'client_id': client_id,
            'transaction': transaction,
            'timestamp': timestamp
        }
        
        self.transaction_queue.put(request_msg)
        print(f"Node {self.node_id} queued transaction: {transaction}")
        
    def transaction_processor(self):
        """Process transactions from the queue sequentially"""
        debug_print(f"[DEBUG] Node {self.node_id} transaction processor started")
        while True:
            try:
                # Wait for a transaction in the queue
                request_msg = self.transaction_queue.get(timeout=1.0)
                debug_print(f"[DEBUG] Node {self.node_id} processing transaction from queue: {request_msg}")
                
                with self.processing_lock:
                    if not self.is_leader:
                        # If we're no longer the leader, forward to current leader
                        debug_print(f"[DEBUG] Node {self.node_id} not leader, forwarding to leader")
                        self.forward_to_leader(request_msg)
                        continue
                    
                    # Process the transaction
                    debug_print(f"[DEBUG] Node {self.node_id} processing transaction as leader")
                    self.process_single_transaction(request_msg)
                    
                self.transaction_queue.task_done()
                
            except Exception as e:
                # Timeout or other error, continue (this is normal when no transactions are queued)
                # Only print actual errors, not timeouts
                if "timeout" not in str(e).lower() and "empty" not in str(e).lower():
                    debug_print(f"[DEBUG] Node {self.node_id} transaction processor error: {e}")
                continue
                
    def process_single_transaction(self, request_msg):
        """Process a single transaction through Paxos consensus"""
        client_id = request_msg['client_id']
        transaction = request_msg['transaction']
        timestamp = request_msg['timestamp']
        
        # Assign sequence number
        current_seq = self.sequence_number
        self.sequence_number += 1
        
        # Store in pending requests
        self.pending_requests[current_seq] = request_msg
        self.log.append({'type': 'REQUEST', 'sequence': current_seq, 'request': request_msg})
        
        # Send ACCEPT message
        accept_msg = {
            'type': 'ACCEPT',
            'ballot': self.ballot_number,
            'sequence': current_seq,
            'request': request_msg,
            'checkpoint_sequence': self.checkpoint_sequence
        }
        
        self.log.append({'type': 'ACCEPT_SENT', 'ballot': self.ballot_number, 'sequence': current_seq})
        self.broadcast(accept_msg)
        
    def forward_to_leader(self, message):
        leader_port = self.find_leader_port()
        self.send_message(leader_port, message)
            
    def find_leader_port(self):
        # Use the shared leader_id to get the leader's port
        # No need to check connectivity since leader is always accessible
        return 8000 + PaxosNode.leader_id
        
    def broadcast_accept(self):
        if not self.pending_requests:
            return
            
        for seq_num, request in self.pending_requests.items():
            accept_msg = {
                'type': 'ACCEPT',
                'ballot': self.ballot_number,
                'sequence': seq_num,
                'request': request,
                'checkpoint_sequence': self.checkpoint_sequence
            }
            self.broadcast(accept_msg)

    def handle_prepare(self, message):
        ballot = message['ballot']
        
        self.log.append({'type': 'PREPARE', 'ballot': ballot, 'from_node': ballot[1]})
        
        if ballot > self.promised_number:
            self.promised_number = ballot
            self.reset_timer()
            
            promise_msg = {
                'type': 'PROMISE',
                'ballot': ballot,
                'accepted_log': self.accepted_log,
                'checkpoint_sequence': self.checkpoint_sequence
            }
            
            self.log.append({'type': 'PROMISE', 'ballot': ballot, 'to_node': ballot[1]})
            
            sender_port = self.find_port_by_ballot(ballot[1])
            if sender_port:
                self.send_message(sender_port, promise_msg)

    def handle_promise(self, message):
        ballot = message['ballot']
        accepted_log = message['accepted_log']
        checkpoint_seq = message.get('checkpoint_sequence', 0)
        
        self.log.append({'type': 'PROMISE_RECEIVED', 'ballot': ballot, 'from_node': ballot[1]})
        
        if ballot == self.ballot_number:
            self.accepted_log.extend(accepted_log)
            
            if len(self.accepted_log) >= MAJORITY - 1:
                self.become_leader()
                self.send_new_view()
                
    def become_leader(self):
        self.is_leader = True
        PaxosNode.leader_id = self.node_id  # Update shared leader tracking
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
            'log': new_view_log,
            'checkpoint_sequence': self.checkpoint_sequence
        }
        
        self.log.append({'type': 'NEW_VIEW_SENT', 'ballot': self.ballot_number, 'log_entries': len(new_view_log)})
        self.new_view_messages.append(new_view_msg)
        self.broadcast(new_view_msg)
        
        for ballot, seq, request in new_view_log:
            if request.get('type') != 'NO_OP':
                self.pending_requests[seq] = request
                
    def handle_new_view(self, message):
        ballot = message['ballot']
        log = message['log']
        checkpoint_seq = message.get('checkpoint_sequence', 0)
        
        self.log.append({'type': 'NEW_VIEW_RECEIVED', 'ballot': ballot, 'from_node': ballot[1]})
        
        if ballot >= self.promised_number:
            self.promised_number = ballot
            self.accepted_log = [(ballot, seq, req) for ballot, seq, req in log]
            self.new_view_messages.append(message)
            
            # Update shared leader tracking
            PaxosNode.leader_id = ballot[1]
            
            # Update checkpoint sequence if higher
            if checkpoint_seq > self.checkpoint_sequence:
                self.checkpoint_sequence = checkpoint_seq
            
            for ballot, seq, request in log:
                if request.get('type') != 'NO_OP':
                    self.pending_requests[seq] = request

    def handle_accept(self, message):
        ballot = message['ballot']
        sequence = message['sequence']
        request = message['request']
        
        # Convert list to tuple for ballot comparison
        if isinstance(ballot, list):
            ballot = tuple(ballot)
        
        self.log.append({'type': 'ACCEPT_RECEIVED', 'ballot': ballot, 'sequence': sequence, 'from_node': ballot[1]})
        
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
            
            self.log.append({'type': 'ACCEPTED_SENT', 'ballot': ballot, 'sequence': sequence, 'to_node': ballot[1]})
            
            sender_port = self.find_port_by_ballot(ballot[1])
            if sender_port:
                self.send_message(sender_port, accepted_msg)

    def handle_accepted(self, message):
        ballot = message['ballot']
        sequence = message['sequence']
        request = message['request']
        node_id = message['node_id']
        
        # Convert list to tuple for ballot comparison
        if isinstance(ballot, list):
            ballot = tuple(ballot)
        
        self.log.append({'type': 'ACCEPTED_RECEIVED', 'ballot': ballot, 'sequence': sequence, 'from_node': node_id})
        
        if ballot == self.ballot_number and self.is_leader:
            if sequence not in self.pending_requests:
                return
                
            # Count ACCEPTED messages for this sequence
            accepted_count = sum(1 for entry in self.log 
                               if (entry.get('type') == 'ACCEPTED_RECEIVED' and 
                                   entry.get('ballot') == ballot and 
                                   entry.get('sequence') == sequence))
            
            if accepted_count >= MAJORITY - 1:
                self.commit_transaction(sequence, request)
                
    def commit_transaction(self, sequence, request):
        # Check if this sequence has already been executed
        if sequence <= self.executed_sequence:
            return
            
        # Check if this sequence has already been committed
        if not hasattr(self, 'committed_sequences'):
            self.committed_sequences = set()
        if sequence in self.committed_sequences:
            return
            
        # Mark this sequence as committed
        self.committed_sequences.add(sequence)
        
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
        
        self.log.append({'type': 'COMMIT_SENT', 'ballot': self.ballot_number, 'sequence': sequence})
        self.broadcast(commit_msg)
        self.send_reply(client_id, timestamp, result)
        
        # Create checkpoint every 3 transactions (bonus feature)
        if self.executed_sequence % 3 == 0 and self.executed_sequence > 0:
            self.create_checkpoint()
        
        if sequence in self.pending_requests:
            del self.pending_requests[sequence]

    def handle_commit(self, message):
        ballot = message['ballot']
        sequence = message['sequence']
        request = message['request']
        
        # Convert list to tuple for ballot comparison
        if isinstance(ballot, list):
            ballot = tuple(ballot)
        
        self.log.append({'type': 'COMMIT_RECEIVED', 'ballot': ballot, 'sequence': sequence, 'from_node': ballot[1]})
        
        # Check if this sequence has already been executed
        if sequence <= self.executed_sequence:
            return
            
        # Check if this sequence has already been committed
        if not hasattr(self, 'committed_sequences'):
            self.committed_sequences = set()
        if sequence in self.committed_sequences:
            return
            
        # Mark this sequence as committed
        self.committed_sequences.add(sequence)
        
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
        
        # Send reply to client using 5000 range ports
        client_port = 5000 + client_id
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(('localhost', client_port))
            sock.send(json.dumps(reply_msg).encode())
            sock.close()
        except Exception as e:
            pass
        
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
            
    def handle_leader_failure(self):
        """Handle leader failure command - make node act like disconnected node"""
        print(f"Node {self.node_id} failed (LF command received)")
        self.is_failed = True
        self.is_leader = False
        self.peers = []  # Remove all peer connections
        # Stop the timer to prevent ballot number increases
        self.timer = None
        
    def recover_from_failure(self):
        """Recover from failure when node becomes live again"""
        print(f"Node {self.node_id} recovered from failure")
        self.is_failed = False
        # Timer will be reset when node becomes leader again
        
    def catch_up_with_peers(self, live_nodes):
        """Catch up with peers when node becomes live again using checkpoint mechanism"""
        debug_print(f"[DEBUG] Node {self.node_id} catch_up_with_peers called with live_nodes: {live_nodes}")
        debug_print(f"[DEBUG] Node {self.node_id} current peers: {self.peers}")
        debug_print(f"[DEBUG] Node {self.node_id} current state: executed_sequence={self.executed_sequence}, checkpoint_sequence={self.checkpoint_sequence}")
        
        if not self.peers:
            debug_print(f"[DEBUG] Node {self.node_id} has no peers for catch-up")
            return
            
        debug_print(f"[DEBUG] Node {self.node_id} starting catch-up process with peers: {self.peers}")
        
        # Request latest checkpoint and missing log entries from a peer
        for peer_port in self.peers:
            try:
                # Request the latest checkpoint and missing log entries
                catch_up_msg = {
                    'type': 'CATCH_UP_REQUEST',
                    'requester_id': self.node_id,
                    'current_sequence': self.executed_sequence,
                    'current_checkpoint_sequence': self.checkpoint_sequence
                }
                debug_print(f"[DEBUG] Node {self.node_id} attempting to send catch-up request to peer {peer_port}: {catch_up_msg}")
                self.send_message(peer_port, catch_up_msg)
                debug_print(f"[DEBUG] Node {self.node_id} sent catch-up request to peer {peer_port}")
                break  # Only request from one peer
            except Exception as e:
                print(f"Node {self.node_id} failed to send catch-up request to peer {peer_port}: {e}")
                continue
                
    def handle_catch_up_request(self, message):
        """Handle catch-up request from a recovering node using checkpoint mechanism"""
        requester_id = message['requester_id']
        requester_sequence = message['current_sequence']
        requester_checkpoint_sequence = message.get('current_checkpoint_sequence', 0)
        
        debug_print(f"[DEBUG] Node {self.node_id} received catch-up request from Node {requester_id}")
        debug_print(f"[DEBUG] Requester sequence: {requester_sequence}, checkpoint: {requester_checkpoint_sequence}")
        debug_print(f"[DEBUG] Current sequence: {self.executed_sequence}, checkpoint: {self.checkpoint_sequence}")
        debug_print(f"[DEBUG] Node {self.node_id} has checkpoint: {self.last_checkpoint is not None}")
        
        # Determine the best catch-up strategy based on checkpoint availability
        if self.checkpoint_sequence > requester_checkpoint_sequence and self.last_checkpoint:
            # Use checkpoint-based catch-up (more efficient)
            debug_print(f"[DEBUG] Node {self.node_id} using checkpoint-based catch-up")
            
            # Send checkpoint state and missing log entries after checkpoint
            missing_logs = []
            for log_entry in self.log:
                if log_entry.get('sequence', 0) > self.checkpoint_sequence:
                    missing_logs.append(log_entry)
            
            catch_up_response = {
                'type': 'CATCH_UP_RESPONSE',
                'responder_id': self.node_id,
                'catch_up_method': 'checkpoint',
                'checkpoint_sequence': self.checkpoint_sequence,
                'checkpoint_digest': self.checkpoint_digest,
                'checkpoint_state': self.last_checkpoint.copy(),
                'missing_logs': missing_logs,
                'current_executed_sequence': self.executed_sequence
            }
            debug_print(f"[DEBUG] Node {self.node_id} sending checkpoint-based response with {len(missing_logs)} missing logs")
        else:
            # Fall back to full state transfer
            debug_print(f"[DEBUG] Node {self.node_id} using full state catch-up")
            
            # Send missing log entries and current state
            missing_logs = []
            for log_entry in self.log:
                if log_entry.get('sequence', 0) > requester_sequence:
                    missing_logs.append(log_entry)
            
            catch_up_response = {
                'type': 'CATCH_UP_RESPONSE',
                'responder_id': self.node_id,
                'catch_up_method': 'full_state',
                'missing_logs': missing_logs,
                'current_datastore': self.datastore.copy(),
                'current_executed_sequence': self.executed_sequence,
                'current_checkpoint_sequence': self.checkpoint_sequence,
                'current_checkpoint_digest': self.checkpoint_digest
            }
            debug_print(f"[DEBUG] Node {self.node_id} sending full state response with {len(missing_logs)} missing logs")
        
        # Send response back to requester
        requester_port = 8000 + requester_id
        self.send_message(requester_port, catch_up_response)
        print(f"Node {self.node_id} sent catch-up response to Node {requester_id}")
        
    def handle_catch_up_response(self, message):
        """Handle catch-up response from a peer using checkpoint mechanism"""
        responder_id = message['responder_id']
        catch_up_method = message.get('catch_up_method', 'full_state')
        
        debug_print(f"[DEBUG] Node {self.node_id} received catch-up response from Node {responder_id} using {catch_up_method}")
        debug_print(f"[DEBUG] Node {self.node_id} before catch-up: executed_sequence={self.executed_sequence}, checkpoint_sequence={self.checkpoint_sequence}")
        
        if catch_up_method == 'checkpoint':
            # Checkpoint-based catch-up
            checkpoint_sequence = message['checkpoint_sequence']
            checkpoint_digest = message['checkpoint_digest']
            checkpoint_state = message['checkpoint_state']
            missing_logs = message['missing_logs']
            current_executed_sequence = message['current_executed_sequence']
            
            debug_print(f"[DEBUG] Node {self.node_id} applying checkpoint-based catch-up")
            debug_print(f"[DEBUG] Checkpoint sequence: {checkpoint_sequence}, missing logs: {len(missing_logs)}")
            debug_print(f"[DEBUG] Checkpoint state: {checkpoint_state}")
            
            # Update checkpoint information
            self.checkpoint_sequence = checkpoint_sequence
            self.checkpoint_digest = checkpoint_digest
            self.last_checkpoint = checkpoint_state.copy()
            
            # Start from checkpoint state
            self.datastore = checkpoint_state.copy()
            debug_print(f"[DEBUG] Node {self.node_id} updated datastore from checkpoint: {self.datastore}")
            
            # Apply missing log entries after checkpoint
            debug_print(f"[DEBUG] Node {self.node_id} applying {len(missing_logs)} missing log entries")
            for i, log_entry in enumerate(missing_logs):
                if log_entry not in self.log:
                    self.log.append(log_entry)
                    debug_print(f"[DEBUG] Node {self.node_id} added log entry {i+1}: {log_entry}")
                    # Apply the transaction if it's a commit
                    if log_entry.get('type') == 'COMMIT':
                        request = log_entry.get('request', {})
                        if request and request.get('type') == 'REQUEST':
                            transaction = request.get('transaction')
                            if transaction:
                                debug_print(f"[DEBUG] Node {self.node_id} applying transaction during catch-up: {transaction}")
                                self.apply_transaction(transaction)
                                debug_print(f"[DEBUG] Node {self.node_id} datastore after transaction: {self.datastore}")
            
            self.executed_sequence = current_executed_sequence
            debug_print(f"[DEBUG] Node {self.node_id} catch-up complete: executed_sequence={self.executed_sequence}")
            
        else:
            # Full state catch-up (fallback)
            missing_logs = message['missing_logs']
            current_datastore = message['current_datastore']
            current_executed_sequence = message['current_executed_sequence']
            current_checkpoint_sequence = message['current_checkpoint_sequence']
            current_checkpoint_digest = message['current_checkpoint_digest']
            
            print(f"Node {self.node_id} applying full state catch-up")
            
            # Update our state with the received information
            self.datastore = current_datastore.copy()
            self.executed_sequence = current_executed_sequence
            self.checkpoint_sequence = current_checkpoint_sequence
            self.checkpoint_digest = current_checkpoint_digest
            
            # Add missing log entries
            for log_entry in missing_logs:
                if log_entry not in self.log:
                    self.log.append(log_entry)
        
        print(f"Node {self.node_id} caught up: executed_sequence={self.executed_sequence}, datastore updated")
        
    def apply_transaction(self, transaction):
        """Apply a transaction to the datastore"""
        sender, receiver, amount = transaction
        sender_key = f'client_{sender}'
        receiver_key = f'client_{receiver}'
        
        if sender_key in self.datastore and receiver_key in self.datastore:
            if self.datastore[sender_key] >= amount:
                self.datastore[sender_key] -= amount
                self.datastore[receiver_key] += amount
                print(f"Node {self.node_id} applied transaction {transaction}: {sender_key}={self.datastore[sender_key]}, {receiver_key}={self.datastore[receiver_key]}")
            else:
                print(f"Node {self.node_id} insufficient funds for transaction {transaction}")
            
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
        
        self.log.append({'type': 'LEADER_ELECTION_STARTED', 'ballot': self.ballot_number})
        
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
        self.received_replies = {}
        self.listener_port = 5000 + client_id
        self.listener_socket = None
        self.listener_thread = None
        self.start_listener()
        
    def start_listener(self):
        try:
            self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener_socket.bind(('localhost', self.listener_port))
            self.listener_socket.listen(5)
            self.listener_thread = threading.Thread(target=self.listen_for_replies, daemon=True)
            self.listener_thread.start()
        except Exception as e:
            print(f"Client {self.client_id} failed to start listener: {e}")
            
    def listen_for_replies(self):
        while True:
            try:
                conn, addr = self.listener_socket.accept()
                data = conn.recv(1024)
                if data:
                    message = json.loads(data.decode())
                    self.handle_reply(message)
                conn.close()
            except Exception as e:
                break
                
    def send_request(self, transaction):
        self.timestamp += 1
        request_msg = {
            'type': 'REQUEST',
            'client_id': self.client_id,
            'transaction': transaction,
            'timestamp': self.timestamp,
            'client_port': self.listener_port
        }
        
        self.pending_requests[self.timestamp] = {'start_time': time.time(), 'retries': 0}
        
        # Send to current leader using shared leader_id
        leader_port = 8000 + PaxosNode.leader_id
        self.send_to_node(leader_port, request_msg)
        
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
        # Check if we received a reply for this timestamp
        if timestamp in self.pending_requests and timestamp not in self.received_replies:
            # Check if the request failed due to insufficient balance
            if timestamp in self.received_replies and self.received_replies[timestamp] == 'failed':
                # Don't retry failed requests due to insufficient balance
                del self.pending_requests[timestamp]
                debug_print(f"[DEBUG] Client {self.client_id} not retrying failed request for timestamp {timestamp}")
                return
            
            # Increment retry count
            self.pending_requests[timestamp]['retries'] += 1
            max_retries = 3  # Maximum number of retries
            
            if self.pending_requests[timestamp]['retries'] > max_retries:
                debug_print(f"[DEBUG] Client {self.client_id} max retries ({max_retries}) exceeded for timestamp {timestamp}, giving up")
                del self.pending_requests[timestamp]
                return
            
            debug_print(f"[DEBUG] Client {self.client_id} retrying request for timestamp {timestamp} (attempt {self.pending_requests[timestamp]['retries']}/{max_retries})")
            self.broadcast_request(timestamp)
            
    def broadcast_request(self, timestamp):
        request_msg = {
            'type': 'REQUEST',
            'client_id': self.client_id,
            'transaction': self.get_transaction_by_timestamp(timestamp),
            'timestamp': timestamp,
            'client_port': self.listener_port
        }
        
        for node_port in self.nodes:
            self.send_to_node(node_port, request_msg)
            
    def get_transaction_by_timestamp(self, timestamp):
        return None
        
    def handle_reply(self, message):
        timestamp = message['timestamp']
        result = message['result']
        
        # Store the received reply
        self.received_replies[timestamp] = result
        
        # Remove from pending requests to stop retries
        if timestamp in self.pending_requests:
            del self.pending_requests[timestamp]
            print(f"Client {self.client_id} received reply for timestamp {timestamp}: {result}")
            print(f"Client {self.client_id} stopped timer for timestamp {timestamp}")
            
    def cleanup(self):
        if self.listener_socket:
            try:
                self.listener_socket.close()
            except:
                pass

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
                    # Handle both old format (1,2,3) and new format (n1,n2,n3)
                    if 'n' in live_nodes_str:
                        # New format: n1, n2, n3, n4, n5
                        live_nodes = [int(x.replace('n', '')) for x in live_nodes_str.split(',') if x]
                    else:
                        # Old format: 1, 2, 3, 4, 5
                        live_nodes = [int(x) for x in live_nodes_str.split(',') if x]
                else:
                    live_nodes = []
                if current_set not in test_sets:
                    test_sets[current_set] = {'transactions': [], 'live_nodes': live_nodes}
            
            if len(row) > 1 and row[1]:
                transaction_str = row[1].strip()
                if transaction_str == 'LF':
                    # Leader failure command
                    test_sets[current_set]['transactions'].append('LF')
                elif transaction_str:
                    # Regular transaction - handle both formats
                    if transaction_str.startswith('(') and transaction_str.endswith(')'):
                        # Format: (A, J, 3) - convert to tuple
                        transaction_str = transaction_str.replace('(', '').replace(')', '')
                        parts = [part.strip() for part in transaction_str.split(',')]
                        if len(parts) == 3:
                            transaction = (parts[0], parts[1], int(parts[2]))
                            test_sets[current_set]['transactions'].append(transaction)
                    else:
                        # Old format: ('A', 'C', 5)
                        transaction = eval(transaction_str)
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
    global DEBUG_MODE
    parser = argparse.ArgumentParser(description='Paxos Consensus Algorithm Implementation')
    parser.add_argument('-d', '--debug', action='store_true', 
                       help='Run in debug mode - automatically continue after 10 seconds instead of waiting for user input')
    args = parser.parse_args()
    DEBUG_MODE = args.debug
    
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

    test_sets = read_input_file('tests/input4.csv')

    for set_number, test_data in test_sets.items():
        debug_print(f"[DEBUG] Running Test Set {set_number}...")
        debug_print(f"[DEBUG] Test Set {set_number} transactions: {test_data['transactions']}")
        debug_print(f"[DEBUG] Test Set {set_number} live_nodes: {test_data['live_nodes']}")
        transactions = test_data['transactions']
        live_nodes = test_data['live_nodes']
        
        # Clear pending requests but keep database state for interdependent test sets
        for node in nodes:
            node.pending_requests = {}
            node.client_replies = {}  # Clear cached client replies
            # Keep datastore, log, executed_sequence, accepted_log, committed_sequences
            # to maintain state across interdependent test sets
        
        # Isolate disconnected nodes by removing them from peer lists
        for node in nodes:
            if node.node_id not in live_nodes:
                # This node is disconnected - isolate it from the network
                node.is_isolated = True
                node.peers = []  # Remove all peers
                node.is_leader = False  # Can't be leader if isolated
                node.timer = None  # Stop timer
                print(f"Node {node.node_id} isolated (disconnected)")
            else:
                # This node is live - ensure it has proper peer connections
                was_isolated = node.is_isolated
                was_failed = node.is_failed
                
                node.is_isolated = False
                # Recover from failure if it was failed
                if node.is_failed:
                    node.recover_from_failure()
                # Restore peer connections to other live nodes
                node.peers = [8000 + peer_id for peer_id in live_nodes if peer_id != node.node_id]
                print(f"Node {node.node_id} connected to peers: {node.peers}")
                
                # If node was previously isolated or failed, trigger catch-up
                if was_isolated or was_failed:
                    time.sleep(2)  # Give more time for peer connections to establish
                    debug_print(f"[DEBUG] Node {node.node_id} triggering catch-up (was_isolated={was_isolated}, was_failed={was_failed})")
                    debug_print(f"[DEBUG] Node {node.node_id} current state before catch-up: executed_sequence={node.executed_sequence}, checkpoint_sequence={node.checkpoint_sequence}")
                    debug_print(f"[DEBUG] Node {node.node_id} current datastore before catch-up: {node.datastore}")
                    node.catch_up_with_peers(live_nodes)
                    debug_print(f"[DEBUG] Node {node.node_id} catch-up call completed")
        
        clients = []
        for i in range(NUM_CLIENTS):
            client = Client(i, [8000 + node_id for node_id in live_nodes])
            clients.append(client)
        
        # Process transactions sequentially, including LF commands
        debug_print(f"[DEBUG] Processing {len(transactions)} transactions in Test Set {set_number}")
        for i, transaction in enumerate(transactions):
            debug_print(f"[DEBUG] Processing transaction {i+1}/{len(transactions)}: {transaction}")
            if transaction == 'LF':
                # Leader failure command - send to current leader
                current_leader = None
                for node in nodes:
                    if node.is_leader and not node.is_isolated and not node.is_failed:
                        current_leader = node
                        break
                
                if current_leader:
                    debug_print(f"[DEBUG] Sending LF command to leader Node {current_leader.node_id}")
                    leader_port = 8000 + current_leader.node_id
                    message = {'type': 'LEADER_FAIL'}
                    send_message_to_node(leader_port, message)
                    time.sleep(2)  # Wait for leader to fail
                else:
                    print("No active leader found for LF command")
            else:
                # Regular transaction
                sender, receiver, amount = transaction
                client_id = ord(sender) - ord('A')
                debug_print(f"[DEBUG] Sending transaction {transaction} from client {client_id} (sender: {sender})")
                clients[client_id].send_request(transaction)
                time.sleep(1.0)  # Delay to allow proper queuing
        
        # Wait for all transactions to be processed
        time.sleep(5)
        
        # Stop all timers to prevent timer expirations during result presentation
        for node in nodes:
            node.timer = None  # Stop the timer
        
        # Cleanup clients
        for client in clients:
            client.cleanup()

        while True:
            if args.debug:
                print(f"\nTest Set {set_number} executed. Debug mode: automatically continuing in 10 seconds...")
                print(f"\nDatabase after Test Set {set_number}:")
                print_db()
                time.sleep(10)
                break
            else:
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
