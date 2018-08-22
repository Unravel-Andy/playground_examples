import os
import zipfile
from subprocess import Popen, PIPE
import utilities

main_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(main_dir)
benchmark_folder = 'demo-packages-master'


def prep_autoaction():
    print('Preparing autoaction benchmark')
    if not os.path.isdir(benchmark_folder):
        zip_target = zipfile.ZipFile('demo-packages-master.zip', 'r')
        zip_target.extractall('./')
        zip_target.close()
    Popen('chmod +x -R %s/auto-actions' % benchmark_folder, shell=True, stdout=PIPE, stderr=PIPE).communicate()
    os.chdir(benchmark_folder + '/auto-actions/setup')
    Popen('bash setup-all', shell=True, stdout=PIPE, stderr=PIPE).communicate()
    os.chdir(main_dir)


def run_autoaction():
    os.chdir(benchmark_folder + '/auto-actions/demos')
    FNULL = open(os.devnull, 'w')
    for file in os.listdir(os.getcwd()):
        if not os.path.isdir(file) and file != 'run-all-demos':
            benchmark_popen = Popen('bash %s' % file, shell=True, stderr=FNULL, stdout=FNULL)
            try:
                while benchmark_popen.poll() is None:
                    utilities.wait_animation('{0} is running PID {1}'.format(file, benchmark_popen.pid))
            except KeyboardInterrupt:
                benchmark_popen.terminate()
                return
    os.chdir(main_dir)


def cleanup_autoaction():
    os.chdir(benchmark_folder + '/auto-actions/setup')
    Popen('bash clean-all', shell=True, stdout=PIPE, stderr=PIPE).communicate()
    os.chdir(main_dir)


def autoaction_example():
    prep_autoaction()
    try:
        run_autoaction()
    except:
        pass
    cleanup_autoaction()