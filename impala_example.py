import os
import re
from subprocess import PIPE, Popen
import utilities

main_dir = os.getcwd()
argv = utilities.argv

def run_impala_example():
    os.chdir('impala')
    FNULL = open(os.devnull, 'w')
    if argv.impala_query == 'all':
        impala_popen = Popen("./run_exp.sh tpcds_text_{0} {1}".format(argv.dataset_size, argv.impala_server), shell=True,
                             stderr=FNULL, stdout=FNULL)
    else:
        impala_popen = Popen("./run_one.sh tpcds_text_{0} query{1}.sql {2}".format(argv.dataset_size, argv.impala_query, argv.impala_server), shell=True, stderr=FNULL, stdout=FNULL)
    try:
        while impala_popen.poll() is None:
            utilities.wait_animation('Running Impala query{0}'.format(argv.impala_query))
    except KeyboardInterrupt:
        impala_popen.terminate()
    os.chdir(main_dir)


def impala_example():
    has_tpcds = 'tpcds_text_%s' % str(argv.dataset_size) in \
                Popen('hive -e \'show databases;\'', shell=True, stdout=PIPE).communicate()[0]
    has_tpcds_impala = 'tpcds_text_%s' % str(argv.dataset_size) in \
                       Popen('impala-shell -i %s -q \'show databases;\'' % argv.impala_server, shell=True,
                             stdout=PIPE).communicate()[0]
    if not has_tpcds:
        utilities.download_benchmark()
        utilities.build_tpcds()
        utilities.setup_tpcds(argv.hive_host, argv.dataset_size)
    if not has_tpcds_impala:
        Popen('impala-shell -i %s -q \'invalidate metadata;\'' % argv.impala_server, shell=True).communicate()[0]
    run_impala_example()
