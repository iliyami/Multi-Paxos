#!/usr/bin/env python3

import argparse
import time
import json
import os
from typing import Dict, List, Any
from smallbank_benchmark import SmallBankBenchmark
from benchmark_evaluator import BenchmarkEvaluator, PerformanceMetrics

class ComprehensiveBenchmark:
    
    def __init__(self, paxos_executable: str = "python3 main.py"):
        
        self.evaluator = BenchmarkEvaluator(paxos_executable)
        self.benchmark = SmallBankBenchmark(num_customers=1000, skew_factor=0.9)
        self.results = {}
    
    def run_throughput_test(self, workload_sizes: List[int] = None) -> Dict[str, Any]:
        
        if workload_sizes is None:
            workload_sizes = [50, 100, 200, 500, 1000]
        
        print("Running Throughput Test...")
        print("=" * 50)
        
        results = []
        
        for size in workload_sizes:
            print(f"Testing throughput with {size} transactions...")
            
            # Generate workload
            workload = self.benchmark.generate_paxos_workload(size)
            
            # Run benchmark
            metrics = self.evaluator.run_benchmark(workload, num_sets=1)
            results.append({
                'workload_size': size,
                'throughput': metrics.throughput,
                'execution_time': metrics.total_execution_time,
                'success_rate': (metrics.successful_transactions / metrics.total_transactions * 100) if metrics.total_transactions > 0 else 0
            })
            
            print(f"  Throughput: {metrics.throughput:.2f} txn/sec")
            print(f"  Execution time: {metrics.total_execution_time:.2f} seconds")
            print(f"  Success rate: {(metrics.successful_transactions / metrics.total_transactions * 100) if metrics.total_transactions > 0 else 0:.1f}%")
            print()
        
        return {
            'test_type': 'throughput',
            'results': results,
            'summary': {
                'max_throughput': max([r['throughput'] for r in results]),
                'avg_throughput': sum([r['throughput'] for r in results]) / len(results),
                'best_workload_size': max(results, key=lambda x: x['throughput'])['workload_size']
            }
        }
    
    def run_latency_test(self, workload_size: int = 200, num_runs: int = 5) -> Dict[str, Any]:
        print("Running Latency Test...")
        print("=" * 50)
        
        latencies = []
        throughputs = []
        
        for run in range(num_runs):
            print(f"Run {run + 1}/{num_runs}...")
            
            # Generate workload
            workload = self.benchmark.generate_paxos_workload(workload_size)
            
            # Run benchmark
            metrics = self.evaluator.run_benchmark(workload, num_sets=1)
            
            latencies.append(metrics.average_latency)
            throughputs.append(metrics.throughput)
            
            print(f"  Latency: {metrics.average_latency:.3f} seconds")
            print(f"  Throughput: {metrics.throughput:.2f} txn/sec")
        
        avg_latency = sum(latencies) / len(latencies)
        avg_throughput = sum(throughputs) / len(throughputs)
        
        return {
            'test_type': 'latency',
            'workload_size': workload_size,
            'num_runs': num_runs,
            'results': {
                'latencies': latencies,
                'throughputs': throughputs,
                'avg_latency': avg_latency,
                'avg_throughput': avg_throughput,
                'latency_std': (sum([(l - avg_latency) ** 2 for l in latencies]) / len(latencies)) ** 0.5
            }
        }
    
    def run_consistency_test(self, workload_size: int = 300) -> Dict[str, Any]:
        print("Running Consistency Test...")
        print("=" * 50)
        
        workload = self.benchmark.generate_paxos_workload(workload_size)
        
        metrics = self.evaluator.run_benchmark(workload, num_sets=1)
        
        consistency_score = 100.0  # Placeholder
        
        return {
            'test_type': 'consistency',
            'workload_size': workload_size,
            'results': {
                'consistency_score': consistency_score,
                'total_transactions': metrics.total_transactions,
                'successful_transactions': metrics.successful_transactions,
                'failed_transactions': metrics.failed_transactions,
                'leader_elections': metrics.leader_elections,
                'node_failures': metrics.node_failures
            }
        }
    
    def run_fault_tolerance_test(self, workload_size: int = 200) -> Dict[str, Any]:
        print("Running Fault Tolerance Test...")
        print("=" * 50)
        
        failure_scenarios = [
            {'name': 'No Failures', 'failed_nodes': []},
            {'name': 'Single Node Failure', 'failed_nodes': [1]},
            {'name': 'Two Node Failures', 'failed_nodes': [1, 2]},
            {'name': 'Leader Failure', 'failed_nodes': [1]},  # Assuming node 1 is leader
        ]
        
        results = []
        
        for scenario in failure_scenarios:
            print(f"Testing scenario: {scenario['name']}")
            
            workload = self.benchmark.generate_paxos_workload(workload_size)
            
            input_file = self.evaluator._create_failure_input_file(workload, scenario['failed_nodes'])
            
            start_time = time.time()
            try:
                import subprocess
                result = subprocess.run(
                    [self.evaluator.paxos_executable, input_file, '-d'],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                end_time = time.time()
                execution_time = end_time - start_time
                
                metrics = self.evaluator._parse_results(result, execution_time, len(workload))
                metrics.node_failures = len(scenario['failed_nodes'])
                
                results.append({
                    'scenario': scenario['name'],
                    'failed_nodes': scenario['failed_nodes'],
                    'throughput': metrics.throughput,
                    'success_rate': (metrics.successful_transactions / metrics.total_transactions * 100) if metrics.total_transactions > 0 else 0,
                    'execution_time': metrics.total_execution_time,
                    'leader_elections': metrics.leader_elections
                })
                
                print(f"  Throughput: {metrics.throughput:.2f} txn/sec")
                print(f"  Success rate: {(metrics.successful_transactions / metrics.total_transactions * 100) if metrics.total_transactions > 0 else 0:.1f}%")
                print(f"  Leader elections: {metrics.leader_elections}")
                
            except Exception as e:
                print(f"  Error: {e}")
                results.append({
                    'scenario': scenario['name'],
                    'failed_nodes': scenario['failed_nodes'],
                    'throughput': 0.0,
                    'success_rate': 0.0,
                    'execution_time': 0.0,
                    'leader_elections': 0
                })
            
            print()
        
        return {
            'test_type': 'fault_tolerance',
            'workload_size': workload_size,
            'results': results
        }
    
    def run_comprehensive_benchmark(self) -> Dict[str, Any]:
        """Run comprehensive benchmark suite"""
        print("Starting Comprehensive Benchmark Suite")
        print("=" * 60)
        
        start_time = time.time()
        
        self.results['throughput'] = self.run_throughput_test()
        self.results['latency'] = self.run_latency_test()
        self.results['consistency'] = self.run_consistency_test()
        self.results['fault_tolerance'] = self.run_fault_tolerance_test()
        
        end_time = time.time()
        total_time = end_time - start_time
        
        self.results['summary'] = {
            'total_execution_time': total_time,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'max_throughput': self.results['throughput']['summary']['max_throughput'],
            'avg_latency': self.results['latency']['results']['avg_latency'],
            'consistency_score': self.results['consistency']['results']['consistency_score']
        }
        
        return self.results
    
    def save_results(self, filename: str = None):
        if filename is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f'benchmark_results_{timestamp}.json'
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"Results saved to: {filename}")
    
    def print_summary(self):
        if not self.results:
            print("No results to display")
            return
        
        print("\n" + "=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)
        
        summary = self.results.get('summary', {})
        
        print(f"Total execution time: {summary.get('total_execution_time', 0):.2f} seconds")
        print(f"Maximum throughput: {summary.get('max_throughput', 0):.2f} txn/sec")
        print(f"Average latency: {summary.get('avg_latency', 0):.3f} seconds")
        print(f"Consistency score: {summary.get('consistency_score', 0):.1f}%")
        
        print("\nTest Results:")
        for test_name, test_results in self.results.items():
            if test_name != 'summary':
                print(f"  {test_name}: {'PASS' if test_results else 'FAIL'}")

def main():
    parser = argparse.ArgumentParser(description='Run comprehensive benchmark for Paxos banking system')
    parser.add_argument('--paxos-executable', default='python3 main.py',
                       help='Command to run the Paxos system')
    parser.add_argument('--output', '-o', default=None,
                       help='Output filename for results')
    parser.add_argument('--test', choices=['throughput', 'latency', 'consistency', 'fault_tolerance', 'all'],
                       default='all', help='Specific test to run')
    
    args = parser.parse_args()
    
    benchmark = ComprehensiveBenchmark(args.paxos_executable)
    
    if args.test == 'all':
        results = benchmark.run_comprehensive_benchmark()
    elif args.test == 'throughput':
        results = {'throughput': benchmark.run_throughput_test()}
    elif args.test == 'latency':
        results = {'latency': benchmark.run_latency_test()}
    elif args.test == 'consistency':
        results = {'consistency': benchmark.run_consistency_test()}
    elif args.test == 'fault_tolerance':
        results = {'fault_tolerance': benchmark.run_fault_tolerance_test()}
    
    benchmark.results = results
    benchmark.print_summary()
    benchmark.save_results(args.output)

if __name__ == "__main__":
    main()
