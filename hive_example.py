import os
from subprocess import Popen, PIPE
import utilities
from time import sleep

main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)


def prepare_hive_benchmark():
    pass


def run_hive_benchmark():
    FNULL = open(os.devnull, 'w')
    os.chdir('sample-queries-tpcds')
    hive_popen = Popen('hive -i init76Pre.txt -f query76.sql', stderr=FNULL, stdout=FNULL, shell=True)
    try:
        while hive_popen.poll() is None:
            utilities.wait_animation('Hive benchmark before is running PID %s ' % hive_popen.pid)
        print('\nHive benchmark before Done')
    except KeyboardInterrupt:
        hive_popen.terminate()
        exit()

    hive_popen = Popen('hive -i init76Post.txt -f query76.sql', stderr=FNULL, stdout=FNULL, shell=True)
    try:
        while hive_popen.poll() is None:
            utilities.wait_animation('Hive benchmark after is running PID %s ' % hive_popen.pid)
        print('\nHive benchmark after Done')
    except KeyboardInterrupt:
        hive_popen.terminate()
        exit()
    os.chdir(main_dir)



def hive_example():
    has_tpcds = 'tpcds_text_100' in \
                Popen('hive -e \'show databases;\'', shell=True, stdout=PIPE).communicate()[0]
    if not has_tpcds:
        print("Hive before after require tpcds 100, will start generating tpcds in 10 seconds")
        sleep(10)
        utilities.download_benchmark()
        utilities.build_tpcds()
        utilities.setup_tpcds(utilities.argv.hive_host, utilities.argv.dataset_size)
    prepare_hive_benchmark()
    run_hive_benchmark()
    pass