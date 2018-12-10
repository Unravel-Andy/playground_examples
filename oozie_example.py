import os
from subprocess import Popen, PIPE
from shutil import copyfile
import utilities

benchmark_folder = 'tagged_wf_sla_input'
main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)


def download_benchmark():
    """
    Download oozie benchmark from preview server
    """
    if not os.path.isdir(benchmark_folder):
        download_popen = Popen('curl https://preview.unraveldata.com/img/tagged_wf_sla_input.tgz -o %s.tgz' % benchmark_folder, shell=True, stdout=PIPE)
        download_popen.wait()
        if download_popen.returncode != 0:
            print('Download oozie demo failed')
            exit(1)
        else:
            print('Download oozie demo success')
        untar_popen = Popen('tar -xvzf %s.tgz' % benchmark_folder, shell=True, stdout=PIPE)
        untar_popen.wait()
        if untar_popen.returncode != 0:
            print('Extract oozie demo tar failed')
            exit(2)


def prep_benchmark():
    os.chdir(os.path.join(main_dir, benchmark_folder))
    tag_properties = """com.unraveldata.tagging.script.enabled=true
com.unraveldata.app.tagging.script.path=/tmp/tag_app.py
com.unraveldata.app.tagging.script.method.name=get_tags
"""
    unravel_prop_path ='/usr/local/unravel/etc/unravel.properties'
    print('Copying tag_app.py to /tmp')
    copyfile('tag_app.py', '/tmp/tag_app.py')
    if os.path.exists(unravel_prop_path):
        with open(unravel_prop_path, 'a+') as f:
            unravel_properties = f.read()
            if tag_properties not in unravel_properties:
                print('Updating unravel.properties')
                f.write(tag_properties)
                restart_daemons()
            f.close()
    else:
        print('unravel.properties file not found exiting')
        exit()


def restart_daemons():
    print('Restarting unravel_jcw2 and unravel_sw')
    Popen('for i in {1..4}; do sudo service unravel_jcw2_$i restart; done', shell=True, stdout=PIPE).wait()
    Popen('for i in {1..4}; do sudo service unravel_sw_$i restart; done', shell=True, stdout=PIPE).wait()
    os.chdir(main_dir)


def run_benchmark():
    os.chdir(os.path.join(main_dir, benchmark_folder))
    FNULL = open(os.devnull, 'w')
    benchmark_popen = Popen('./run_tagged_wf_sla_input.spark-submit.bash -tmp-hdfs /tmp/abc -wf-name wf-sla-mr-spark -project-name test -dept-name eng', shell=True, stdout=FNULL, stderr=FNULL)
    try:
        while benchmark_popen.poll() is None:
            utilities.wait_animation('Oozie benchmark running PID %s ' % benchmark_popen.pid)
        print('\nOozie benchmark Done')
    except KeyboardInterrupt:
        benchmark_popen.terminate()
        exit()
    os.chdir(main_dir)


def oozie_example():
    download_benchmark()
    prep_benchmark()
    run_benchmark()