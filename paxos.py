import time
from dataclasses import dataclass
from typing import List, Tuple, Optional

@dataclass
class Transaction:
    sender: int
    receiver: int
    amount: int
    sequence: int

@dataclass
class Block:
    sequence_number: int
    transactions: List[Transaction]

import socket
import pickle
import server
from typing import List, Tuple, Optional

class Paxos:
    def __init__(self, server):
        self.server = server
        self.ballot_number = (0, self.server.id)
        self.accepted_value: Optional[Block] = None
        self.promised_ballot = None
        self.last_committed_ballot = (0, 0)
        self.timer_duration = 5  # seconds

    def broadcast(self, message):
        for server in self.server.get_live_servers():
            if server.id != self.server.id:
                self.send_message(server, message)

    def send_message(self, target_server_id, message):
        target_server = server.Server.get_server(target_server_id)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_server.host, target_server.port))
            s.sendall(pickle.dumps(message))
            response = pickle.loads(s.recv(4096))
        return response

    def send_prepare(self) -> List[Tuple[int, Optional[Block], List[Transaction]]]:
        prepare_message = {
            'type': 'prepare',
            'ballot_number': self.ballot_number,
            'last_committed_ballot': self.last_committed_ballot
        }
        responses = []
        start_time = time.time()

        while time.time() - start_time < self.timer_duration:
            for server_id in self.server.get_live_servers():
                if server_id != self.server.id:
                    response = self.send_message(server_id, prepare_message)
                    if response:
                        responses.append(response)
            
            if len(responses) >= self.server.quorum_size:
                break
            time.sleep(0.1)

        return responses

    def handle_prepare(self, prepare_message):
        ballot_number = prepare_message['ballot_number']
        sender_last_committed = prepare_message['last_committed_ballot']

        if ballot_number > self.promised_ballot and sender_last_committed >= self.last_committed_ballot:
            self.promised_ballot = ballot_number
            return (self.server.id, self.accepted_value, self.server.local_log)
        return None

    def send_accept(self, block):
        accept_message = {
            'type': 'accept',
            'ballot_number': self.ballot_number,
            'block': block
        }
        accepted_count = 1  # Count self
        for server in self.server.get_live_servers():
            if server.id != self.server.id:
                if self.send_message(server, accept_message):
                    accepted_count += 1
        
        return accepted_count >= self.server.quorum_size

    def handle_accept(self, accept_message):
        if accept_message['ballot_number'] >= self.promised_ballot:
            self.accepted_value = accept_message['block']
            return True
        return False

    def send_commit(self, block):
        commit_message = {
            'type': 'commit',
            'ballot_number': self.ballot_number,
            'block': block
        }
        self.broadcast(commit_message)
        self.handle_commit(commit_message)

    def handle_commit(self, commit_message):
        block = commit_message['block']
        self.server.datastore.append_block(block)
        self.server.remove_committed_transactions(block.transactions)
        self.last_committed_ballot = commit_message['ballot_number']
        self.accepted_value = None
        self.promised_ballot = None

    def initiate_consensus(self, local_transactions: List[Transaction]) -> bool:
        self.ballot_number = (self.ballot_number[0] + 1, self.server.id)
        prepare_responses = self.send_prepare()
        
        if len(prepare_responses) < self.server.quorum_size:
            return False

        major_block = self.construct_major_block(prepare_responses, local_transactions)
        if self.send_accept(major_block):
            self.send_commit(major_block)
            return True
        return False