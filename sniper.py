import commands
import json
import sys
import syslog
import yaml

_TRANSACTIONS = 'TRANSACTIONS'
def run():
    stream = file('db_process.yml', 'r')
    config = yaml.load(stream)['production']
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
                        syslog.syslog(syslog.LOG_ERR, 'sniper.py failed to execute kill order on %d: %s' % (pid, ret[1]))
                    else:
                        syslog.syslog(syslog.LOG_ERR, 'sniper.py killed %d that ran %d seconds' % (pid, process['time']))
    else:
        syslog.syslog(syslog.LOG_ERR, 'sniper.py could found no data from the parser')

if __name__ == '__main__':
    run()
