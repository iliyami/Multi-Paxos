import csv
import threading
from threading import Barrier
from server import Server
from client import Client


def read_input_file(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        test_sets = {}
        current_set = None
        for row in reader:
            if row[0]:  # New set
                current_set = int(row[0])
                if current_set not in test_sets:
                    test_sets[current_set] = {'transactions': [], 'live_servers': eval(row[2])}
            test_sets[current_set]['transactions'].append(eval(row[1]))
    return test_sets

TOTAL_SERVERS = 5
startup_barrier = Barrier(TOTAL_SERVERS + 1)
servers = [Server(i, 'localhost', 5000+i, TOTAL_SERVERS, startup_barrier) for i in range(1, TOTAL_SERVERS+1)]
clients = [Client(i, 'localhost', 5000+i) for i in range(1, TOTAL_SERVERS+1)]
# Start servers in separate threads
server_threads = [threading.Thread(target=server.start) for server in servers]
for thread in server_threads:
    thread.start()
startup_barrier.wait()
test_sets = read_input_file('tests/input.csv')
for set_number, test_set in test_sets.items():
    input(f"Press Enter to process Set {set_number}")
    
    # Update live servers
    for server in servers:
        server.live_servers = set(test_set['live_servers'])
    for transaction in test_set['transactions']:
        sender, receiver, amount = transaction
        client = clients[sender-1]
        response = client.send_transaction(receiver, amount)
        print(f"Transaction {transaction}: {response}")
    while True:
        print("\nAvailable commands:")
        print("1. PrintBalance <server_id>")
        print("2. PrintLog <server_id>")
        print("3. PrintDB <server_id>")
        print("4. Performance")
        print("5. Continue to next set")
        
        command = input("Enter command: ").split()
        
        if command[0] == "PrintBalance" and len(command) == 2:
            server_id = int(command[1])
            servers[server_id-1].print_balance()
        elif command[0] == "PrintLog" and len(command) == 2:
            server_id = int(command[1])
            servers[server_id-1].print_log()
        elif command[0] == "PrintDB" and len(command) == 2:
            server_id = int(command[1])
            servers[server_id-1].print_db()
        elif command[0] == "Performance":
            # Implement performance measurement
            print("Performance measurement not implemented yet")
        elif command[0] == "Continue":
            break
        else:
            print("Invalid command")
