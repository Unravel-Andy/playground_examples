import re
import os
import glob
import traceback
import zipfile
from subprocess import PIPE, Popen
import utilities

main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)
benchmark_folder = 'streaming-app-example-with-sources'


def download_streaming_demo():
    """
    Download zip file contains all the spark streaming source code from preview
    """
    if not os.path.isdir(benchmark_folder):
        download_popen = Popen('curl https://preview.unraveldata.com/img/streaming-app-example-with-sources.zip -o %s.zip' % benchmark_folder, shell=True, stdout=PIPE)
        download_popen.communicate()

        if download_popen.returncode != 0:
            print('Download Spark Streaming benchmark failed')
            exit(1)
        else:
            print('Download Spark Streaming benchmark success')

        ss_demo_zip = zipfile.ZipFile('%s.zip' % benchmark_folder, 'r')
        ss_demo_zip.extractall(os.getcwd())
        ss_demo_zip.close()


def prep_streaming_demo(hdfs_master='localhost'):
    os.chdir(benchmark_folder)
    cluster_type = utilities.cluster_type()
    with open('streaming-app.sh', 'r+') as f:
        streaming_app_content = f.read()
        if cluster_type == 'OTHER':
            sensor_path = 'UNRAVEL_SENSOR_PATH=/usr/local/unravel-agent/jars/'
        elif cluster_type == "CDH":
            sensor_path = 'UNRAVEL_SENSOR_PATH=/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/'
        streaming_app_content = re.sub('UNRAVEL_SENSOR_PATH=.*', sensor_path, streaming_app_content)

        HDFS_MASTER = 'HDFS_MASTER="hdfs://%s:8020"' % hdfs_master
        streaming_app_content = re.sub('HDFS_MASTER=.*', HDFS_MASTER, streaming_app_content)

        src_zip = 'SRC_ZIP={0}'.format(os.path.join(os.getcwd(), 'jar/benchmarks-src.zip'))
        streaming_app_content = re.sub('SRC_ZIP=.*', src_zip, streaming_app_content)
        f.seek(0)
        f.truncate()
        f.write(streaming_app_content)
        f.close()

    with open('producer.sh', 'r+') as f:
        producer_content = f.read()
        spark_assembly = 'export SPARK_ASSEMBLY=%s' % Popen('sudo find / -name \'spark-assembly*.jar\'', shell=True, stdout=PIPE, stderr=PIPE).communicate()[0].split('\n')[0]
        producer_content = re.sub('export SPARK_ASSEMBLY=.*', spark_assembly, producer_content)
        if cluster_type == 'CDH':
            cdh_lib = 'export CDH_LIB=/opt/cloudera/parcels/CDH/lib/hadoop/client'
        else:
            cdh_lib = 'export CDH_LIB=%s' % glob.glob('/usr/hdp/[2-3]*/falcon/client/lib/')[0]
        producer_content = re.sub('export CDH_LIB=.*', cdh_lib, producer_content)
        f.seek(0)
        f.truncate()
        f.write(producer_content)
    os.chdir(main_dir)


def run_streaming_demo():
    os.chdir(os.path.join(main_dir, benchmark_folder))
    FNULL = open(os.devnull, 'w')
    streaming_app_popen = Popen('./streaming-app.sh', stdout=FNULL, stderr=FNULL, shell=True)
    producer_popen = Popen('./producer.sh', stdout=FNULL, stderr=FNULL, shell=True)
    try:
        while streaming_app_popen.poll() is None:
            utilities.wait_animation('Spark streaming job is running PID {0} {1}'.format(streaming_app_popen.pid, producer_popen.pid))
        producer_popen.terminate()
    except:
        traceback.print_exc()
        streaming_app_popen.terminate()
        producer_popen.terminate()
    os.chdir(main_dir)
    print('\nSpark streaming demo completed')


def spark_streaming_example():
    download_streaming_demo()
    prep_streaming_demo()
    run_streaming_demo()
