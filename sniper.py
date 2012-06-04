import commands
import json
import sys
import logging
import yaml

_TRANSACTIONS = 'TRANSACTIONS'
def run():
    stream = file('db_process.yml', 'r')
    config = yaml.load(stream)['production']
    logging.basicConfig(filename=config['log'])
    input = sys.stdin.read()
    temp = json.loads(input)
    if _TRANSACTIONS in temp:
        data = temp[_TRANSACTIONS]
        for process in data:
            if process['time'] > config['TTL']:
                if 'tables' in process or 'num_locks' in process:
                    pid = int(process['pid'])
                    ret = commands.getstatusoutput('mysql -u%s -p%s -s -e "KILL %d"' % (config['user'], config['password'], pid))
                    if ret[0] != 0:
                        logging.error('sniper.py failed to execute kill order on %d: %s' % (pid, ret[1]))
                    else:
                        logging.warn('sniper.py killed %d that ran %d seconds' % (pid, process['time']))
    else:
        logging.error('sniper.py could found no data from the parser')

if __name__ == '__main__':
    run()
