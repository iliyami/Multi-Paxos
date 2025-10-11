# Paxos Banking System with SmallBank Benchmark

A distributed banking application implementing the Paxos consensus protocol for state machine replication, with comprehensive performance evaluation using the SmallBank benchmark.

## Overview

This project implements a fault-tolerant distributed banking system using the Paxos consensus algorithm. The system ensures that all banking transactions are consistently maintained across multiple nodes (replicas) through consensus-based state machine replication.

## Features

- **Paxos Consensus Protocol**: Implements leader election, normal operations, and node failure handling
- **Distributed Banking**: Supports 10 clients (A-J) with transfer transactions
- **Fault Tolerance**: Handles leader failures, node isolation, and recovery
- **Checkpointing (Bonus 1)**: Periodic state saving for efficient recovery and log optimization
- **SmallBank Benchmark (Bonus 2)**: Comprehensive performance evaluation framework

## System Architecture

- **5 Paxos Nodes**: Distributed across ports 8001-8005
- **10 Banking Clients**: Clients A through J with initial balance of 10 units each
- **Leader-based Consensus**: Single leader coordinates all transactions
- **Majority-based Decisions**: Requires 3 out of 5 nodes for consensus

## Key Components

### Core Paxos Implementation (`main.py`)
- Leader election with prepare/promise messages
- Transaction processing with accept/accepted/commit phases
- Node failure detection and recovery
- State synchronization and catch-up mechanisms
- **Checkpointing system** for efficient state recovery

### SmallBank Benchmark
- **`smallbank_benchmark.py`**: Generates realistic banking workloads
- **`benchmark_evaluator.py`**: Performance metrics collection
- **`run_benchmark.py`**: Comprehensive benchmark suite
- **6 Transaction Types**: Amalgamate, Balance, DepositChecking, SendPayment, TransactSavings, WriteCheck

## Performance Results

The system achieves excellent performance with the SmallBank benchmark:
- **Maximum Throughput**: 330.50 transactions/second
- **Perfect Reliability**: 100% success rate across all tests
- **Linear Scaling**: Throughput scales with workload size
- **Consistent Execution**: ~2.6 seconds regardless of workload

## Usage

### Run Basic Tests
```bash
python3 main.py tests/input4.csv -d
```

### Generate SmallBank Workload
```bash
python3 smallbank_benchmark.py
```

### Run Performance Benchmark
```bash
python3 run_benchmark.py --test throughput
```

### Run Comprehensive Benchmark
```bash
python3 run_benchmark.py --test all
```

## Test Scenarios

The system handles various scenarios including:
- Normal transaction processing
- Leader failures and elections
- Node isolation and recovery
- Concurrent leader elections
- Insufficient nodes for consensus
- State synchronization after failures

## Files

- `main.py` - Core Paxos implementation
- `smallbank_benchmark.py` - SmallBank benchmark implementation
- `benchmark_evaluator.py` - Performance evaluation framework
- `run_benchmark.py` - Benchmark runner
- `tests/input4.csv` - Test scenarios

## Bonus Implementations

### Bonus 1: Checkpointing
- **Periodic state saving** after every 100 committed requests
- **Efficient recovery** using checkpoints instead of processing all previous requests
- **Log optimization** with checkpoint-based catch-up mechanisms
- **State synchronization** for failed nodes using checkpoint data

### Bonus 2: SmallBank Benchmark
- **Comprehensive performance evaluation** using industry-standard benchmark
- **Realistic banking workloads** with 6 transaction types and skewed access patterns
- **Performance metrics** including throughput, latency, and consistency
- **Scalability testing** with varying workload sizes and failure scenarios

## Academic Context

This project implements the Paxos consensus protocol as described in the course materials, with additional SmallBank benchmark evaluation following the paper "Serializable isolation for snapshot databases" by Michael J Cahill, Uwe Röhm, and Alan D Fekete. At the end I have used AI assistance for creating this readme and understanding the benchmark behavior and context.
