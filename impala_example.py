import os
import re
from subprocess import PIPE, Popen
import utilities

main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)


def run_impala_example():
    os.chdir('impala')
    FNULL = open(os.devnull, 'w')
    if utilities.argv.impala_query == 'all':
        impala_popen = Popen("./run_exp.sh tpcds_text_{0} {1}".format(utilities.argv.dataset_size, utilities.argv.impala_server), shell=True,
                             stderr=FNULL, stdout=FNULL)
    else:
        if utilities.argv.impala_query < 10:
            utilities.argv.impala_query = '{:02d}'.format(utilities.argv.impala_query)
        impala_popen = Popen("./run_one.sh tpcds_text_{0} query{1}.sql {2}".format(utilities.argv.dataset_size, utilities.argv.impala_query, utilities.argv.impala_server), shell=True, stderr=FNULL, stdout=FNULL)
    try:
        while impala_popen.poll() is None:
            utilities.wait_animation('Running Impala query{0}'.format(utilities.argv.impala_query))
    except KeyboardInterrupt:
        impala_popen.terminate()
        exit()
    os.chdir(main_dir)
    print('\nImpala query complete')


def impala_example():
    has_tpcds = 'tpcds_text_{0}'.format(utilities.argv.dataset_size) in \
                Popen('hive -e \'show databases;\'', shell=True, stdout=PIPE, stderr=PIPE).communicate()[0]
    has_tpcds_impala = 'tpcds_text_{0}'.format(utilities.argv.dataset_size) in \
                       Popen('impala-shell -i %s -q \'show databases;\'' % utilities.argv.impala_server, shell=True,
                             stdout=PIPE, stderr=PIPE).communicate()[0]
    if not has_tpcds:
        utilities.download_benchmark()
        utilities.build_tpcds()
        utilities.setup_tpcds(utilities.argv.hive_host, utilities.argv.dataset_size)
    if not has_tpcds_impala:
        Popen('impala-shell -i %s -q \'invalidate metadata;\'' % utilities.argv.impala_server, shell=True).communicate()[0]
    run_impala_example()
