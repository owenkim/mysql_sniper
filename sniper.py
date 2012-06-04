import commands
import json
import sys
import yaml

_TRANSACTIONS = 'TRANSACTIONS'
def run():
    stream = file('db_process.yml', 'r')
    config = yaml.load(stream)['production']
    input = sys.stdin.read()
    data = json.loads(input)[_TRANSACTIONS]
    for process in data:
        if process['time'] > config['TTL']:
            if 'tables' in process or 'num_locks' in process:
                pid = int(process['pid'])
                print pid
                ret = commands.getstatusoutput('mysql -u%s -p%s -s -e "KILL %d"' % (config['user'], config['password'], pid))
                if ret[0] != 0:
                    print 'Failed to execute kill order on %d: %s' % (pid, ret[1])

if __name__ == '__main__':
    run()
