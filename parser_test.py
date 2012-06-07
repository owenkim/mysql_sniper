import unittest
import StringIO
from parser import MySqlParser

_EXPECTED_TRANSACTIONS = [{'tables': [], 'pid': '2', 'time': 5000},
                          {'num_locks': 300, 'tables': [], 'pid': '5', 'time': 30},
                          {'num_locks': 300, 'tables': ['tablefoo', 'tablefoo'], 'pid': '4', 'time': 300},
                          {'tables': [], 'pid': '7', 'time': 1}]

class TestMySqlParser(unittest.TestCase):

    def setUp(self):
        self.parser = MySqlParser(StringIO.StringIO(_TEST_BUFFER))

    def test_initial_parse(self):
        data = self.parser.initial_parse()
        self.assertIn("BACKGROUND THREAD", data.keys())
        self.assertIn("SEMAPHORES", data.keys())
        self.assertIn("TRANSACTIONS", data.keys())
        self.assertIn("FILE I/O", data.keys())
        self.assertIn("INSERT BUFFER AND ADAPTIVE HASH INDEX", data.keys())
        self.assertIn("LOG", data.keys())
        self.assertIn("BUFFER POOL AND MEMORY", data.keys())
        self.assertIn("ROW OPERATIONS", data.keys())

    def test_parse_transactions(self):
        data = self.parser.parse_transactions(_TEST_TRANSACTIONS)
        self.assertEqual(_EXPECTED_TRANSACTIONS, data)


_TEST_BUFFER = """
*************************** 1. row ***************************
  Type: InnoDB
  Name:
Status:
=====================================
120601 12:02:19 INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 54 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 9 1_second, 9 sleeps, 0 10_second, 11 background, 11 flush
srv_master_thread log flush and writes: 9
----------
SEMAPHORES
----------
OS WAIT ARRAY INFO: reservation count 5, signal count 5
Mutex spin waits 3, rounds 31, OS waits 1
RW-shared spins 4, rounds 120, OS waits 4
RW-excl spins 0, rounds 0, OS waits 0
Spin rounds per wait: 10.33 mutex, 30.00 RW-shared, 0.00 RW-excl
------------
TRANSACTIONS
------------
Trx id counter 69307
Purge done for trx's n:o < 69107 undo n:o < 0
History list length 1807
LIST OF TRANSACTIONS FOR EACH SESSION:
---TRANSACTION 0, not started
MySQL thread id 10, OS thread handle 0x1110f1000, query id 152 localhost root
SHOW ENGINE INNODB STATUS
---TRANSACTION 69306, ACTIVE 127 sec
3 lock struct(s), heap size 376, 4 row lock(s), undo log entries 1
MySQL thread id 6, OS thread handle 0x1110ae000, query id 147 localhost root
--------
FILE I/O
--------
I/O thread 0 state: waiting for i/o request (insert buffer thread)
I/O thread 1 state: waiting for i/o request (log thread)
I/O thread 2 state: waiting for i/o request (read thread)
I/O thread 3 state: waiting for i/o request (read thread)
I/O thread 4 state: waiting for i/o request (read thread)
I/O thread 5 state: waiting for i/o request (read thread)
I/O thread 6 state: waiting for i/o request (write thread)
I/O thread 7 state: waiting for i/o request (write thread)
I/O thread 8 state: waiting for i/o request (write thread)
I/O thread 9 state: waiting for i/o request (write thread)
Pending normal aio reads: 0 [0, 0, 0, 0] , aio writes: 0 [0, 0, 0, 0] ,
 ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
Pending flushes (fsync) log: 0; buffer pool: 0
822 OS file reads, 15 OS file writes, 11 OS fsyncs
0.00 reads/s, 0 avg bytes/read, 0.00 writes/s, 0.00 fsyncs/s
-------------------------------------
INSERT BUFFER AND ADAPTIVE HASH INDEX
-------------------------------------
Ibuf: size 1, free list len 0, seg size 2, 0 merges
merged operations:
 insert 0, delete mark 0, delete 0
discarded operations:
 insert 0, delete mark 0, delete 0
Hash table size 276671, node heap has 2 buffer(s)
0.00 hash searches/s, 0.00 non-hash searches/s
---
LOG
---
Log sequence number 952752940
Log flushed up to   952752940
Last checkpoint at  952752940
0 pending log writes, 0 pending chkp writes
12 log i/o's done, 0.00 log i/o's/second
----------------------
BUFFER POOL AND MEMORY
----------------------
Total memory allocated 137363456; in additional pool allocated 0
Dictionary memory allocated 1064494
Buffer pool size   8191
Free buffers       7378
Database pages     811
Old database pages 319
Modified db pages  0
Pending reads 0
Pending writes: LRU 0, flush list 0, single page 0
Pages made young 0, not young 0
0.00 youngs/s, 0.00 non-youngs/s
Pages read 811, created 0, written 6
0.00 reads/s, 0.00 creates/s, 0.00 writes/s
No buffer pool page gets since the last printout
Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
LRU len: 811, unzip_LRU len: 0
I/O sum[0]:cur[0], unzip sum[0]:cur[0]
--------------
ROW OPERATIONS
--------------
0 queries inside InnoDB, 0 queries in queue
1 read views open inside InnoDB
Main thread id 4560420864, state: waiting for server activity
Number of rows inserted 2, updated 0, deleted 0, read 18
0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
----------------------------
END OF INNODB MONITOR OUTPUT
============================
"""

_TEST_TRANSACTIONS = [
'---TRANSACTION 0 533800919, not started, process no 3998, OS thread id 139795632027392',
'MySQL thread id 1, query id 75877679 foo.bar.internal 127.0.0.1 db',
'---TRANSACTION 0 533801029, ACTIVE 5000 sec, process no 3998, OS thread id 139794339448576',
'MySQL thread id 2, query id 75878099 foo.bar.internal 127.0.0.1 db',
'---TRANSACTION 0 533801321, not started, process no 3998, OS thread id 139795628816128',
'MySQL thread id 3, query id 75879169 foo.bar.internal 127.0.0.1 db',
'---TRANSACTION 0 533801326, ACTIVE 30 sec, process no 3998, OS thread id 139792822892288',
'4 lock struct(s), heap size 1216, 300 row lock(s), undo log entries 1',
'MySQL thread id 5, query id 75879225 foo.bar.internal 127.0.0.1 db',
'Trx read view will not see trx with id >= 0 533801327, sees < 0 533801281',
'---TRANSACTION 0 533801326, ACTIVE 300 sec, process no 3998, OS thread id 139792822892288',
'4 lock struct(s), heap size 1216, 300 row lock(s), undo log entries 1',
'MySQL thread id 4, query id 75879225 foo.bar.internal 127.0.0.1 db',
'Trx read view will not see trx with id >= 0 533801327, sees < 0 533801281',
'TABLE LOCK table `db`.`tablefoo` trx id 7CD32 lock mode IX',
'RECORD LOCKS space id 0 page no 1349 n bits 72 index `GEN_CLUST_INDEX` of table `db`.`tablefoo` trx id 7CD32 lock_mode X',
'Record lock, heap no 1 PHYSICAL RECORD: n_fields 1; compact format; info bits 0',
' 0: len 8; hex 73757072656d756d; asc supremum;;',
'',
'Record lock, heap no 2 PHYSICAL RECORD: n_fields 4; compact format; info bits 0',
' 0: len 6; hex 000000007c00; asc     | ;;',
' 1: len 6; hex 000000069101; asc       ;;',
' 2: len 7; hex 820000018b0110; asc        ;;',
' 3: len 4; hex 80000001; asc     ;;',
'',
'RECORD LOCKS space id 0 page no 1349 n bits 72 index `GEN_CLUST_INDEX` of table `db`.`tablefoo` trx id 7CD32 lock_mode X locks gap before rec',
'Record lock, heap no 5 PHYSICAL RECORD: n_fields 4; compact format; info bits 0',
' 0: len 6; hex 000000009d76; asc      v;;',
' 1: len 6; hex 00000007cd32; asc      2;;',
' 2: len 7; hex cf000001650110; asc     e  ;;',
' 3: len 4; hex 80000065; asc    e;;',
'',
'---TRANSACTION 0 533801281, ACTIVE 1 sec, process no 3998, OS thread id 139795632629504',
'MySQL thread id 7, query id 75879221 foo.bar.internal 127.0.0.1 db',
'Trx read view will not see trx with id >= 0 533801282, sees < 0 533801001'
]



if __name__ == '__main__':
    unittest.main()
