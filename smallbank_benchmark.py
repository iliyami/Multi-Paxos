#!/usr/bin/env python3

import random
import time
import json
import csv
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from enum import Enum

class TransactionType(Enum):
    AMALGAMATE = "Amalgamate"
    BALANCE = "Balance"
    DEPOSIT_CHECKING = "DepositChecking"
    SEND_PAYMENT = "SendPayment"
    TRANSACT_SAVINGS = "TransactSavings"
    WRITE_CHECK = "WriteCheck"

@dataclass
class SmallBankTransaction:
    """Represents a SmallBank transaction"""
    type: TransactionType
    customer_id: int
    customer_id_2: int = None  # For transactions involving two customers
    amount: float = 0.0
    timestamp: float = 0.0

class SmallBankBenchmark:
    def __init__(self, num_customers: int = 1000, skew_factor: float = 0.9):
        self.num_customers = num_customers
        self.skew_factor = skew_factor
        self.transaction_weights = {
            TransactionType.AMALGAMATE: 0.05,      # 5%
            TransactionType.BALANCE: 0.20,         # 20%
            TransactionType.DEPOSIT_CHECKING: 0.20, # 20%
            TransactionType.SEND_PAYMENT: 0.25,    # 25%
            TransactionType.TRANSACT_SAVINGS: 0.15, # 15%
            TransactionType.WRITE_CHECK: 0.15      # 15%
        }
        
        # Initialize customer data
        self.customers = self._initialize_customers()
        
        # Statistics
        self.stats = {
            'total_transactions': 0,
            'transaction_counts': {ttype: 0 for ttype in TransactionType},
            'execution_times': [],
            'errors': 0
        }
    
    def _initialize_customers(self) -> Dict[int, Dict[str, Any]]:
        customers = {}
        
        for customer_id in range(1, self.num_customers + 1):
            customers[customer_id] = {
                'customer_id': customer_id,
                'name': f'Customer_{customer_id}',
                'checking_balance': random.uniform(100.0, 10000.0),  # $100-$10,000
                'savings_balance': random.uniform(1000.0, 50000.0),  # $1,000-$50,000
                'access_count': 0  # Track access frequency for skew analysis
            }
        
        return customers
    
    def _get_skewed_customer(self) -> int:
        if random.random() < self.skew_factor:
            # 90% of requests go to first 10% of customers
            return random.randint(1, max(1, self.num_customers // 10))
        else:
            # 10% of requests go to remaining customers
            return random.randint(self.num_customers // 10 + 1, self.num_customers)
    
    def _get_random_customer(self) -> int:
        return random.randint(1, self.num_customers)
    
    def generate_transaction(self) -> SmallBankTransaction:
        # Select transaction type based on weights
        rand = random.random()
        cumulative = 0.0
        selected_type = None
        
        for ttype, weight in self.transaction_weights.items():
            cumulative += weight
            if rand <= cumulative:
                selected_type = ttype
                break
        
        if selected_type == TransactionType.AMALGAMATE:
            customer1 = self._get_skewed_customer()
            customer2 = self._get_random_customer()
            while customer2 == customer1:
                customer2 = self._get_random_customer()
            return SmallBankTransaction(
                type=selected_type,
                customer_id=customer1,
                customer_id_2=customer2,
                timestamp=time.time()
            )
        
        elif selected_type == TransactionType.BALANCE:
            return SmallBankTransaction(
                type=selected_type,
                customer_id=self._get_skewed_customer(),
                timestamp=time.time()
            )
        
        elif selected_type == TransactionType.DEPOSIT_CHECKING:
            return SmallBankTransaction(
                type=selected_type,
                customer_id=self._get_skewed_customer(),
                amount=random.uniform(10.0, 1000.0),
                timestamp=time.time()
            )
        
        elif selected_type == TransactionType.SEND_PAYMENT:
            customer1 = self._get_skewed_customer()
            customer2 = self._get_random_customer()
            while customer2 == customer1:
                customer2 = self._get_random_customer()
            return SmallBankTransaction(
                type=selected_type,
                customer_id=customer1,
                customer_id_2=customer2,
                amount=random.uniform(10.0, 500.0),
                timestamp=time.time()
            )
        
        elif selected_type == TransactionType.TRANSACT_SAVINGS:
            return SmallBankTransaction(
                type=selected_type,
                customer_id=self._get_skewed_customer(),
                amount=random.uniform(-500.0, 1000.0),  # Can be deposit or withdrawal
                timestamp=time.time()
            )
        
        elif selected_type == TransactionType.WRITE_CHECK:
            return SmallBankTransaction(
                type=selected_type,
                customer_id=self._get_skewed_customer(),
                amount=random.uniform(10.0, 200.0),
                timestamp=time.time()
            )
    
    def convert_to_paxos_transaction(self, smallbank_tx: SmallBankTransaction) -> List[Tuple[str, str, float]]:
        paxos_transactions = []
        customer_id = smallbank_tx.customer_id
        
        if smallbank_tx.type == TransactionType.AMALGAMATE:
            # Transfer all money from customer1 to customer2
            customer1 = self.customers[customer_id]
            customer2 = self.customers[smallbank_tx.customer_id_2]
            
            # Transfer checking balance
            if customer1['checking_balance'] > 0:
                paxos_transactions.append((
                    f'client_{customer_id}',
                    f'client_{smallbank_tx.customer_id_2}',
                    customer1['checking_balance']
                ))
            
            # Transfer savings balance
            if customer1['savings_balance'] > 0:
                paxos_transactions.append((
                    f'client_{customer_id}',
                    f'client_{smallbank_tx.customer_id_2}',
                    customer1['savings_balance']
                ))
        
        elif smallbank_tx.type == TransactionType.BALANCE:
            # Balance check - no money transfer, just a read operation
            # In Paxos, we can represent this as a no-op or a small transfer to self
            pass  # No transaction needed for balance check
        
        elif smallbank_tx.type == TransactionType.DEPOSIT_CHECKING:
            # Deposit money into checking account
            # In Paxos, this would be a transfer from a "bank" account to customer
            paxos_transactions.append((
                'client_BANK',  # Bank account
                f'client_{customer_id}',
                smallbank_tx.amount
            ))
        
        elif smallbank_tx.type == TransactionType.SEND_PAYMENT:
            paxos_transactions.append((
                f'client_{customer_id}',
                f'client_{smallbank_tx.customer_id_2}',
                smallbank_tx.amount
            ))
        
        elif smallbank_tx.type == TransactionType.TRANSACT_SAVINGS:
            if smallbank_tx.amount > 0:
                paxos_transactions.append((
                    'client_BANK',  
                    f'client_{customer_id}',
                    smallbank_tx.amount
                ))
            else:
                paxos_transactions.append((
                    f'client_{customer_id}',
                    'client_BANK',  
                    abs(smallbank_tx.amount)
                ))
        
        elif smallbank_tx.type == TransactionType.WRITE_CHECK:
            paxos_transactions.append((
                f'client_{customer_id}',
                'client_BANK', 
                smallbank_tx.amount
            ))
        
        return paxos_transactions
    
    def generate_workload(self, num_transactions: int) -> List[SmallBankTransaction]:
        workload = []
        for _ in range(num_transactions):
            workload.append(self.generate_transaction())
        return workload
    
    def generate_paxos_workload(self, num_transactions: int) -> List[Tuple[str, str, float]]:
        paxos_workload = []
        smallbank_workload = self.generate_workload(num_transactions)
        
        for smallbank_tx in smallbank_workload:
            paxos_txs = self.convert_to_paxos_transaction(smallbank_tx)
            paxos_workload.extend(paxos_txs)
        
        return paxos_workload
    
    def save_workload_to_csv(self, workload: List[SmallBankTransaction], filename: str):
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['transaction_type', 'customer_id', 'customer_id_2', 'amount', 'timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for tx in workload:
                writer.writerow({
                    'transaction_type': tx.type.value,
                    'customer_id': tx.customer_id,
                    'customer_id_2': tx.customer_id_2 if tx.customer_id_2 else '',
                    'amount': tx.amount,
                    'timestamp': tx.timestamp
                })
    
    def save_paxos_workload_to_csv(self, workload: List[Tuple[str, str, float]], filename: str):
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['sender', 'receiver', 'amount']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for sender, receiver, amount in workload:
                writer.writerow({
                    'sender': sender,
                    'receiver': receiver,
                    'amount': amount
                })
    
    def print_statistics(self):
        print("\n=== SmallBank Benchmark Statistics ===")
        print(f"Total customers: {self.num_customers}")
        print(f"Skew factor: {self.skew_factor}")
        print(f"Total transactions generated: {self.stats['total_transactions']}")
        print("\nTransaction type distribution:")
        for ttype, count in self.stats['transaction_counts'].items():
            percentage = (count / self.stats['total_transactions'] * 100) if self.stats['total_transactions'] > 0 else 0
            print(f"  {ttype.value}: {count} ({percentage:.1f}%)")
        
        if self.stats['execution_times']:
            avg_time = sum(self.stats['execution_times']) / len(self.stats['execution_times'])
            print(f"\nAverage execution time: {avg_time:.3f} seconds")
            print(f"Total errors: {self.stats['errors']}")

def main():
    print("SmallBank Benchmark for Paxos Banking System")
    print("=" * 50)
    
    benchmark = SmallBankBenchmark(num_customers=1000, skew_factor=0.9)
    
    num_transactions = 1000
    print(f"Generating {num_transactions} SmallBank transactions...")
    
    smallbank_workload = benchmark.generate_workload(num_transactions)
    benchmark.save_workload_to_csv(smallbank_workload, 'smallbank_workload.csv')
    
    paxos_workload = benchmark.generate_paxos_workload(num_transactions)
    benchmark.save_paxos_workload_to_csv(paxos_workload, 'paxos_workload.csv')
    
    benchmark.stats['total_transactions'] = len(smallbank_workload)
    for tx in smallbank_workload:
        benchmark.stats['transaction_counts'][tx.type] += 1
    
    benchmark.print_statistics()
    
    print(f"\nWorkloads saved to:")
    print(f"  - smallbank_workload.csv ({len(smallbank_workload)} transactions)")
    print(f"  - paxos_workload.csv ({len(paxos_workload)} transactions)")

if __name__ == "__main__":
    main()
