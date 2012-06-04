import commands
import sys
import logging
import yaml

def run():
    stream = file('db_process.yml', 'r')
    config = yaml.load(stream)['production']
    logging.basicConfig(filename=config['log'])
    ret = commands.getstatusoutput('mysql -u%s -p%s -s -e "SHOW ENGINE INNODB STATUS\G"' % (config['user'], config['password']))
    if ret[0] == 0:
        print ret[1]
    else:
        logging.error('fetch_process.py - Failed to fetch DB status: %s' % ret[1])

if __name__ == '__main__':
    run()
