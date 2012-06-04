# This takes the SHOW ENGINE INNODB STATUS output, parses it, and (for now)
# outputs processes all transactions and some metadata related as JSON
import json
import re
import sys

_TRANSACTIONS = 'TRANSACTIONS'
def run():
    result = {}
    data = initialParse()
    if _TRANSACTIONS in data:
        result[_TRANSACTIONS] = parseTransactions(data[_TRANSACTIONS])

    print json.dumps(result)

def initialParse():
    data = {}
    header = False # keep simple state of if we are in a header or not
    headerStr = ""
    bodyList = []
    for line in sys.stdin.readlines():
        line = line.strip()
        if line.startswith('END OF INNODB MONITOR OUTPUT'):
            return data
        if line.startswith('----------'):
            header = not header
            if headerStr:
                data[headerStr] = bodyList
                bodyList = []
        else:
            if header:
                headerStr = line
            else:
                bodyList.append(line)

    # Shouldn't really get here
    return data

# returns a list of dictionaries.
# Each dictionary have keys pid: #pid, locks: tables for which this has RECORD locks,
# time: time in SECONDS that this txn has been active. Locks may not exist if no locks are there
def parseTransactions(lines):
    data = []
    currData = {}
    tables = []
    for line in lines:
        if line.startswith('---TRANSACTION'):
            if 'time' in currData and 'pid' in currData:
                currData['tables'] = tables
                tables = []
                data.append(currData)
                currData = {}
            # TODO: Error checking?
            m = re.search(r'ACTIVE (\d+) sec', line)
            if m: # might be "not active"
                time = int(m.group(1))
                currData['time'] = time
        else:
            if line.startswith('RECORD LOCKS'):
                m = re.search(r'table `[a-z\d_]+`.`([a-z\d_]+)`', line)
                table_name = m.group(1)
                tables.append(table_name)
            elif line.startswith('MySQL thread id'):
                m = re.search(r'MySQL thread id (\d+)', line)
                id = m.group(1)
                currData['pid'] = id
            else:
                m = re.search('(\d+) row lock\(s\)', line)
                if m:
                    num_locks = int(m.group(1))
                    currData['num_locks'] = num_locks
    if 'time' in currData and 'pid' in currData:
        currData['locks'] = tables
        data.append(currData)
    return data

if __name__ == '__main__':
    run()
