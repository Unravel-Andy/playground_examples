import os
import re
from subprocess import PIPE, Popen
import utilities

main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)
benchmark_folder = 'demo-benchmarks-for-spark'

# Download Unravel spark benchmark
def download_benchmark():
    if not os.path.isdir(benchmark_folder):
        download_popen = Popen('curl https://preview.unraveldata.com/img/spark-benchmarks1.tgz -o %s.tgz' % benchmark_folder, shell=True, stdout=PIPE)
        download_popen.communicate()

        if download_popen.returncode != 0:
            print('Download Spark benchmark failed')
            exit(1)
        else:
            print('Download Spark benchmark success')

        untar_popen = Popen('tar -xvzf %s.tgz' % benchmark_folder, shell=True)
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

    os.chdir('benchmarks/scripts')
    with open('example%s.sh' % utilities.argv.spark_example, 'r+') as f:
        example_content = f.read()
        if not '--name' in example_content:
            example_content = re.sub('spark-submit ',
                                     'spark-submit \\\n    --name "example%s-before" ' % utilities.argv.spark_example,
                                     example_content)
        f.seek(0)
        f.truncate()
        f.write(example_content)
        f.close()
    with open('example%s-after.sh' % utilities.argv.spark_example, 'r+') as f:
        example_content = f.read()
        if not '--name' in example_content:
            example_content = re.sub('spark-submit ',
                                     'spark-submit \\\n    --name "example%s-after" ' % utilities.argv.spark_example,
                                     example_content)
        f.seek(0)
        f.truncate()
        f.write(example_content)
        f.close()
    os.chdir(main_dir)


# Run Unravel Benchmark
# Input: example number 1-6
def run_benchmark():
    os.chdir('%s/benchmarks/scripts' % benchmark_folder)
    FNULL = open(os.devnull, 'w')
    before_popen = Popen('./example%s.sh' % str(utilities.argv.spark_example), shell=True, stdout=FNULL, stderr=FNULL)
    try:
        while before_popen.poll() is None:
            utilities.wait_animation('Running example {0} before. PID {1}'.format(utilities.argv.spark_example, before_popen.pid))
        print('\nexample {0} before complete'.format(utilities.argv.spark_example))
    except KeyboardInterrupt:
        before_popen.terminate()
        exit()

    after_popen = Popen('./example%s-after.sh' % str(utilities.argv.spark_example), shell=True, stdout=FNULL, stderr=FNULL)
    try:
        while after_popen.poll() is None:
            utilities.wait_animation('Running example {0} after. PID {1}'.format(utilities.argv.spark_example, after_popen.pid))
        print('\nexample {0} after complete'.format(utilities.argv.spark_example))
    except KeyboardInterrupt:
        after_popen.terminate()
        exit()
    os.chdir(main_dir)


def spark_example():
    download_benchmark()
    prep_benchmark()
    run_benchmark()
