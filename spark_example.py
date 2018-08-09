import os
from subprocess import PIPE, Popen
import utilities

main_dir = os.getcwd()
benchmark_folder = 'demo-benchmarks-for-spark-2.0'
argv = utilities.argv

# Download Unravel spark benchmark
def download_benchmark():
    if not os.path.isdir(benchmark_folder):
        download_popen = Popen('curl http://preview.unraveldata.com/img/demo-benchmarks-for-spark-2.0.tgz -o %s.tgz' % benchmark_folder, shell=True, stdout=PIPE)
        download_popen.communicate()

        if download_popen.returncode != 0:
            print('Download Spark benchmark failed')
            exit(1)

        untar_popen = Popen('tar -xvzf %.tgz' % benchmark_folder, shell=True)
        untar_popen.communicate()
        if untar_popen.returncode != 0:
            print('Extract spark benchmark failed')
            exit(2)

# Put data files to hdfs
def prep_benchmark():
    os.chdir(benchmark_folder)
    print("Uploading spark benchmark data to hdfs")
    Popen('hdfs dfs -put data/tpch10g/ /tmp/', shell=True, stdout=PIPE).wait()
    Popen('hdfs dfs -put data/DATA.BIG.2G/ /tmp/', shell=True, stdout=PIPE).wait()
    os.chdir(main_dir)

# Run Unravel Benchmark
# Input: example number 1-6
def run_benchmark():
    os.chdir('%s/benchmarks/scripts' % benchmark_folder)
    FNULL = open(os.devnull, 'w')
    before_popen = Popen('./example%s.sh' % str(argv.spark_example), shell=True, stdout=FNULL, stderr=FNULL)
    try:
        while before_popen.poll() is None:
            utilities.wait_animation('Running example {0} before. PID {1}'.format(argv.spark_example, before_popen.pid))
    except KeyboardInterrupt:
        before_popen.terminate()

    after_popen = Popen('./example%s-after.sh' % str(argv.spark_example), shell=True, stdout=FNULL, stderr=FNULL)
    try:
        while after_popen.poll() is None:
            utilities.wait_animation('Running example {0} after. PID {1}'.format(argv.spark_example, after_popen.pid))
    except KeyboardInterrupt:
        after_popen.terminate()
    os.chdir(main_dir)


def spark_example():
    download_benchmark()
    prep_benchmark()
    run_benchmark()
