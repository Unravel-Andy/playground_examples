import os
from subprocess import PIPE, Popen
import threading
import spark_example as SE
import hive_example as HE
import oozie_example as OE
import spark_streaming_example as SS
import impala_example as IE
import utilities


def main():
    utilities.args_parser()
    argv = utilities.argv
    script_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_path)
    if argv.spark or argv.hive or argv.workflow or argv.impala or argv.spark_streaming or argv.impala_server or argv.impala_query:
        TEST_ALL = False
    else:
        TEST_ALL = True

    if argv.hive or TEST_ALL:
        HE.hive_example()
        # threading.Thread(target=).start()

    if argv.impala or argv.impala_server != 'localhost' or argv.impala_query != '1' or TEST_ALL:
        IE.impala_example()
        # threading.Thread(target=).start()

    if argv.workflow or TEST_ALL:
        OE.oozie_example()

    if argv.spark or TEST_ALL:
        SE.spark_example()
        # threading.Thread(target=SE.spark_example()).start()

    if argv.spark_streaming or TEST_ALL:
        SS.spark_streaming_example()
        # threading.Thread(target=).start()


if __name__ == '__main__':
    main()
