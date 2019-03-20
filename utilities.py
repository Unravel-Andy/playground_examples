import re
import os
import sys
import argparse
from subprocess import PIPE, Popen
from time import sleep, time

argv = None
main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)


def args_parser():
    global argv
    parser = argparse.ArgumentParser()
    parser.add_argument('-spark', action='store_true')
    parser.add_argument('-hive', action='store_true')
    parser.add_argument('-workflow', action='store_true')
    parser.add_argument('-impala', action='store_true')
    parser.add_argument('-autoaction', action='store_true')
    parser.add_argument('--spark-streaming', action='store_true')
    parser.add_argument('--hive-host', help='Hive server host', default='localhost')
    parser.add_argument('--hdfs-master', help='HDFS master host', default='localhost')
    parser.add_argument('--dataset-size', help='The size of the Dataset is using has to greater than 2 ', default=100)
    parser.add_argument('--impala-server', help='Impala server host', default='localhost')
    parser.add_argument('--impala-query', help='from 1 to 99 or all to run all queries', default=1)
    parser.add_argument('--spark-example', help='from 1 to 5', default=1)
    argv = parser.parse_args()


def cluster_type():
    """
    # Get cluster type currently only support CDH and HDP
    :return: cluster type CDH, HDP, MAPR (Only support CDH at this moment)
    """
    hadoop_version = Popen('hadoop version', shell=True, stdout=PIPE).communicate()[0]
    if re.search('cdh|cloudera', hadoop_version):
        return 'CDH'
    else:
        return 'OTHER'


def download_benchmark():
    """
    # Download TPCDS benchmark from githup
    """
    if not os.path.isdir('hive-testbench'):
        download_popen = Popen('git clone https://github.com/hortonworks/hive-testbench.git', shell=True, stdout=PIPE)
        result = download_popen.communicate()
        if download_popen.returncode != 0:
            print(result[1])
            print('Failed to clone hive benchmark')
            exit(1)


def wait_animation(message, sleep_time=5):
    """
    Spinning animation
    :param message: Message need to print
    :param sleep_time: how long this function will wait for
    :return:
    """
    animation = "|/-\\"
    start_time = 0
    while start_time < sleep_time:
        sys.stdout.write("\r{0} {1}".format(message, animation[int(time()) % len(animation)]))
        sys.stdout.flush()
        start_time += 1
        sleep(1)


def build_tpcds():
    """
    Run tpcds-build.sh script
    """
    os.chdir('hive-testbench')
    FNULL = open(os.devnull, 'w')
    if os.path.exists('tpcds-build.sh'):
        build_popen = Popen(['./tpcds-build.sh'], stderr=FNULL, stdout=FNULL)
        try:
            while build_popen.poll() is None:
                wait_animation('tpcds build in progress')
        except KeyboardInterrupt:
            build_popen.terminate()
            exit()
    else:
        print('tpcds-build.sh not found')
    os.chdir(main_dir)


def setup_tpcds(hive2_host='localhost', data_size=100):
    """
    Run tpcds-setup.sh script
    :param hive2_host: hive server2 hostname
    :param data_size: data set size that will be generated
    """
    os.chdir('hive-testbench')
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
    Popen('sudo -u hdfs hdfs dfs -mkdir /data/', shell=True).communicate()
    Popen('sudo -u hdfs hdfs dfs -mkdir /data/tpcds', shell=True).communicate()
    Popen('sudo -u hdfs hdfs dfs -chmod -R 777 /data/', shell=True).communicate()
    Popen('sudo -u hdfs hdfs dfs -chmod -R 777 /user/hive/warehouse', shell=True).communicate()
    setup_popen = Popen('./tpcds-setup.sh %s /data/tpcds' % data_size, shell=True)
    try:
        setup_popen.communicate()
    except KeyboardInterrupt:
        setup_popen.terminate()
        exit()
    os.chdir(main_dir)
