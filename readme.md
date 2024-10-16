# Modified Paxos Consensus Protocol for Distributed Banking

## Project Overview

This project implements a variant of the Paxos consensus protocol for a distributed banking application. The system consists of 5 servers and 5 clients, where each server is responsible for processing transactions initiated by a single client.

## Features

- Modified Paxos consensus protocol implementation
- Distributed transaction processing
- Fault tolerance for up to 2 server failures
- Catch-up mechanism for recovered servers
- Local transaction logging and global datastore

## Requirements

- Python 3.7+ (or your chosen programming language)
- Network socket library (built-in)

## Setup

1. Clone the repository:


2. Install dependencies:

pip install -r requirements.txt

## Usage

1. Start the servers:

python main.py

2. The program will read from an input CSV file containing sets of transactions.

3. Follow the prompts to process each set of transactions.

4. Use the following commands between transaction sets:
- `PrintBalance <server_id>`: Print the balance of a given client
- `PrintLog <server_id>`: Print the local log of a given server
- `PrintDB <server_id>`: Print the current datastore
- `Performance`: Print throughput and latency metrics


## Implementation Details

- Each server maintains a local transaction log and a global datastore
- The modified Paxos protocol is used for consensus when a client has insufficient balance
- Catch-up mechanism implemented for synchronizing recovered or slow servers
- Quorum construction with timeout for handling slow or crashed servers
- Unique sequence numbers for maintaining log consistency

## Performance

The implementation aims to demonstrate reasonable performance in terms of throughput (transactions committed per second) and latency (average processing time per transaction).

## Bonus Features (Optional)

- [ ] Modified Multi-Paxos protocol
- [ ] Database integration for datastore
- [ ] Efficient balance retrieval across servers

## Deadline

This project is due on October 17, 2024, at 11:59 pm.

## Contributors

- [Iliya Mirzaei]

## License

This project is part of the CSE 535: Distributed Systems course and is subject to the course's academic policies.