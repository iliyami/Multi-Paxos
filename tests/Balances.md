## Balance Calculation Results

### Test Set Analysis (input4.csv)

**Initial Balances:** All clients start with 10 units each

**Key Principles:**
- Majority required: 3 nodes out of 5 total nodes
- Leader election based on highest ballot number (not necessarily Node 1)
- After leader failure (LF), the failed leader becomes unavailable for remainder of test set
- Transactions fail if insufficient nodes for majority consensus

### Test Set Results:

#### Test Set 1: [n1, n2, n3, n4, n5] - All nodes live
- `(A, J, 3)`: A: 10-3=7, J: 10+3=13
- `(C, D, 1)`: C: 10-1=9, D: 10+1=11
- `(B, E, 4)`: B: 10-4=6, E: 10+4=14
- `(F, G, 2)`: F: 10-2=8, G: 10+2=12
- `(H, I, 2)`: H: 10-2=8, I: 10+2=12
- **Result:** A:7, B:6, C:9, D:11, E:14, F:8, G:12, H:8, I:12, J:13

#### Test Set 2: [n1, n2, n3, n4] - Node 5 isolated
- `(B, I, 2)`: B: 6-2=4, I: 12+2=14
- `(J, C, 4)`: J: 13-4=9, C: 9+4=13
- **Result:** A:7, B:4, C:13, D:11, E:14, F:8, G:12, H:8, I:14, J:9

#### Test Set 3: [n1, n3, n5] - Nodes 2,4 isolated
- `(B, F, 3)`: B: 4-3=1, F: 8+3=11
- `(C, A, 2)`: C: 13-2=11, A: 7+2=9
- **Result:** A:9, B:1, C:11, D:11, E:14, F:11, G:12, H:8, I:14, J:9

#### Test Set 4: [n1, n2, n3, n5] - Node 4 isolated
- `(B, F, 3)`: ❌ FAILS (B has insufficient balance: 1 < 3)
- `(A, H, 3)`: A: 9-3=6, H: 8+3=11
- **Result:** A:6, B:1, C:11, D:11, E:14, F:11, G:12, H:11, I:14, J:9

#### Test Set 5: [n1, n2] - Nodes 3,4,5 isolated
- **Available nodes:** 2 < 3 (majority required)
- `(C, B, 1)`: ❌ FAILS (insufficient nodes for majority)
- `(D, F, 2)`: ❌ FAILS (insufficient nodes for majority)
- **Result:** A:6, B:1, C:11, D:11, E:14, F:11, G:12, H:11, I:14, J:9 *(no change)*

#### Test Set 6: [n1, n2, n3, n4, n5] - All nodes live + LF
- `(A, B, 4)`: A: 6-4=2, B: 1+4=5
- `LF` - Leader failure (leader becomes unavailable)
- `(I, D, 2)`: I: 14-2=12, D: 11+2=13
- `(E, J, 2)`: E: 14-2=12, J: 9+2=11
- **Result:** A:2, B:5, C:11, D:13, E:12, F:11, G:12, H:11, I:12, J:11

#### Test Set 7: [n1, n2, n3, n4, n5] - All nodes live
- `(F, A, 2)`: F: 11-2=9, A: 2+2=4
- `(G, H, 1)`: G: 12-1=11, H: 11+1=12
- `(J, A, 2)`: J: 11-2=9, A: 4+2=6
- **Result:** A:6, B:5, C:11, D:13, E:12, F:9, G:11, H:12, I:12, J:9

#### Test Set 8: [n1, n2, n3] - Nodes 4,5 isolated + LF
- `(E, A, 3)`: E: 12-3=9, A: 6+3=9
- `(I, B, 2)`: I: 12-2=10, B: 5+2=7
- `(C, D, 2)`: C: 11-2=9, D: 13+2=15
- `(G, H, 2)`: G: 11-2=9, H: 12+2=14
- `LF` - Leader failure (only 2 nodes remain: insufficient for majority)
- `(F, A, 2)`: ❌ FAILS (insufficient nodes for majority)
- `(J, B, 1)`: ❌ FAILS (insufficient nodes for majority)
- **Result:** A:9, B:7, C:9, D:15, E:9, F:9, G:9, H:14, I:10, J:9

#### Test Set 9: [n1, n2, n3, n4, n5] - All nodes live + 2 LF
- `(C, H, 3)`: C: 9-3=6, H: 14+3=17
- `LF` - First leader failure (4 nodes remain: sufficient for majority)
- `(E, D, 1)`: E: 9-1=8, D: 15+1=16
- `(G, I, 2)`: G: 9-2=7, I: 10+2=12
- `LF` - Second leader failure (3 nodes remain: exactly at majority threshold)
- `(A, J, 1)`: A: 9-1=8, J: 9+1=10
- **Result:** A:8, B:7, C:6, D:16, E:8, F:9, G:7, H:17, I:12, J:10

#### Test Set 10: [n1, n2, n3, n4, n5] - All nodes live
- **50 transactions** in a repeating 10-transaction cycle:
  1. `(A, B, 1)`: A: 8-1=7, B: 7+1=8
  2. `(D, E, 1)`: D: 16-1=15, E: 8+1=9
  3. `(I, J, 1)`: I: 12-1=11, J: 10+1=11
  4. `(J, A, 1)`: J: 11-1=10, A: 7+1=8
  5. `(B, C, 1)`: B: 8-1=7, C: 6+1=7
  6. `(F, G, 1)`: F: 9-1=8, G: 7+1=8
  7. `(H, I, 1)`: H: 17-1=16, I: 11+1=12
  8. `(E, F, 1)`: E: 9-1=8, F: 8+1=9
  9. `(G, H, 1)`: G: 8-1=7, H: 16+1=17
  10. `(C, D, 1)`: C: 7-1=6, D: 15+1=16
- **Key insight:** After one complete 10-transaction cycle, balances return to original state
- **5 complete cycles** (50 transactions total) = **no net change**
- **Result:** A:8, B:7, C:6, D:16, E:8, F:9, G:7, H:17, I:12, J:10 *(same as Test Set 9)*

### Final Balances After Test Set 10:
**A:8, B:7, C:6, D:16, E:8, F:9, G:7, H:17, I:12, J:10**

### Critical Success Criteria:
1. ✅ All transactions execute when majority is available
2. ✅ Transactions fail when insufficient nodes for majority
3. ✅ Leader failures properly isolate failed leaders
4. ✅ New leaders elected from remaining live nodes
5. ✅ System maintains consistency across all test sets