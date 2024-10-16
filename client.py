import socket

class Client:
    def __init__(self, client_id, server_host, server_port):
        self.id = client_id
        self.server_host = server_host
        self.server_port = server_port

    def send_transaction(self, receiver, amount):
        transaction = (self.id, receiver, amount)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.server_host, self.server_port))
            s.sendall(str(transaction).encode())
            response = s.recv(1024).decode()
        return response