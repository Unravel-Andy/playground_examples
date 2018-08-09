import re
import os
import sys
import argparse
from subprocess import PIPE, Popen
from time import sleep, time


def args_parser():
    global argv
    parser = argparse.ArgumentParser()
    parser.add_argument('-spark')
    parser.add_argument('-hive')
    parser.add_argument('-workflow')
    parser.add_argument('--spark-steaming')
    parser.add_argument('--hive-host', default='localhost')
    parser.add_argument('--hdfs-master', default='localhost')
    parser.add_argument('--dateset-size', default=50)
    parser.add_argument('--impala-server', default='localhost')
    parser.add_argument('--impala-query', 'from 1 to 99 or all to run all queries', default=1)
    parser.add_argument('--spark-example', 'from 1 to 6', default=1)
    argv = parser.parse_args()


def cluster_type():
    hadoop_version = Popen('hadoop version', shell=True, stdout=PIPE).communicate()[0]
    if re.search('cdh|cloudera', hadoop_version):
        return 'CDH'
    else:
        return 'OTHER'

def download_benchmark():
    download_popen = Popen('git clone https://github.com/hortonworks/hive-testbench.git', shell=True, stdout=PIPE)
    download_popen.communicate()
    if download_popen.returncode != 0:
        print('Failed to clone hive benchmark')
        exit(1)

def wait_animation(message, sleep_time=5):
    animation = "|/-\\"
    start_time = 0
    while start_time < sleep_time:
        sys.stdout.write("\r{0} {1}".format(message, animation[int(time()) % len(animation)]))
        sys.stdout.flush()
        start_time += 1
        sleep(1)


def build_tpcds():
    if os.path.exists('tpcds-build.sh'):
        build_popen = Popen(['./tpcds-build.sh'], stderr=PIPE, stdout=PIPE)
        try:
            while build_popen.poll() is None:
                wait_animation('tpcds build in progress')
        except KeyboardInterrupt:
            build_popen.terminate()
    else:
        print('tpcds-build.sh not found')


def setup_tpcds(hive2_host='localhost', data_size=100):
    with open('tpcds-setup.sh', 'r+') as f:
        tpcds_content = f.read()
        regex = 'localhost:2181\/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\?tez.queue.name=default'
        f.seek(0)
        f.truncate()
        f.write(re.sub(regex, '%s:10000/' % hive2_host, tpcds_content))
    with open('settings/load-partitioned.sql', 'r+') as f:
        load_sql = f.read()
        load_sql = re.sub('-- set mapreduce.map.memory.mb=.*', 'set mapreduce.map.memory.mb=6094;', load_sql)
        load_sql = re.sub('-- set mapreduce.reduce.memory.mb=.*', 'set mapreduce.reduce.memory.mb=9012;', load_sql)
        f.seek(0)
        f.truncate()
        f.write(load_sql)

    FNULL = open(os.devnull, 'w')
    print(Popen('sudo -u hdfs hdfs dfs -mkdir /data/', shell=True, stdout=PIPE, stderr=PIPE).communicate()[0])
    print(Popen('sudo -u hdfs hdfs dfs -mkdir /data/tpcds', shell=True, stdout=PIPE, stderr=PIPE).communicate()[0])
    print(Popen('sudo -u hdfs hdfs dfs -chmod -R 777 /data/', shell=True, stdout=PIPE, stderr=PIPE).communicate()[0])
    print(Popen('sudo -u hdfs hdfs dfs -chmod -R 777 /user/hive/warehouse', shell=True, stdout=PIPE, stderr=PIPE).communicate()[0])
    setup_popen = Popen('./tpcds-setup.sh %d /data/tpcds' % data_size, shell=True, stdout=PIPE)
    try:
        print(setup_popen.communicate()[0])
    except KeyboardInterrupt:
        setup_popen.terminate()
