import os
import shutil
import random
import string
import subprocess
from datetime import datetime
from pathlib import Path


def random_id(length=8):
    return ''.join(random.sample(string.ascii_letters + string.digits, length))


def get_timestamp():
    return datetime.now().strftime("%Y%m%d-%H%M%S")


QSUB_FILE_NAME = 'qsub_file.qsub'
LOG_PATH = 'logs'

TEMPLATE_BASE = """
#!/bin/sh
# embedded options to qsub - start with #PBS
# -- Name of the job ---
#PBS -N {job_name}
# -- specify queue --
#PBS -q hpc
# -- estimated wall clock time (execution time): hh:mm:ss --
#PBS -l walltime=10:00:00
# -- number of processors/cores/nodes --
#PBS -l nodes=1:ppn=1
# -- user email address --
# please uncomment the following line and put in your e-mail address,
# if you want to receive e-mail notifications on a non-default address
##PBS -M your_email_address
# -- mail notification --
##PBS -m abe
#PBS -o {logfile}
#PBS -e {errfile}
# -- run in the current working (submission) directory --
if test X$PBS_ENVIRONMENT = XPBS_BATCH; then cd $PBS_O_WORKDIR; fi
# here follow the commands you want to execute

echo '**** Activating conda environment ****: env_name = '{env}
source activate {env}
"""

TEMPLATE_QSUB_CODE = TEMPLATE_BASE+"""
echo '**** Running script ****'
{script_call}
echo '**** Script completed ****'
"""

PYTHON_CLEANUP_CODE_TEMPLATE = """
# SCRIPT SUFFIX AUTOMATICALLY ADDED
#import os
#print('removing script file')
#os.remove('{script}')
"""


def prepare_output_location(output_path):
    qsub_output_path = Path(output_path) / 'qsub_files'
    qsub_output_path.mkdir(parents=True, exist_ok=True)
    qsub_output_path.joinpath(LOG_PATH).mkdir(parents=True, exist_ok=True)

    return qsub_output_path


def prepare_script_from_code(code, code_cleanup_template, script_file):
    script_file = str(script_file)

    script_text = code + \
        code_cleanup_template.format(
            script=script_file)

    open(script_file, 'w').write(script_text)


def prepare_qsub_file(qsub_output_path, script_call, job_name, logfile, errfile, env):
    open(str(qsub_output_path / QSUB_FILE_NAME), 'w').write(TEMPLATE_QSUB_CODE.format(script_call=script_call,
                                                                                      job_name=job_name,
                                                                                      logfile=qsub_output_path/LOG_PATH/logfile,
                                                                                      errfile=qsub_output_path/LOG_PATH/errfile,
                                                                                      env=env))


def submit_job(qsub_output_path):
    subprocess.call('qsub < ' + str(qsub_output_path /
                                    QSUB_FILE_NAME), shell=True)


def submit_python_code(code, output_path, script_arguments='', job_name="job", logfile="$PBS_JOBID.output", errfile="$PBS_JOBID.error", env='base'):

    qsub_output_path = prepare_output_location(output_path)

    script_file_name = qsub_output_path / 'job_script.py'

    prepare_script_from_code(
        code, PYTHON_CLEANUP_CODE_TEMPLATE, script_file_name)

    submit_python_script(script_file_name, output_path, script_arguments=script_arguments,
                         job_name=job_name, logfile=logfile, errfile=errfile, env=env)


def submit_bash_code(code, output_path, job_name="job", logfile="$PBS_JOBID.output", errfile="$PBS_JOBID.error", env='base'):

    qsub_output_path = prepare_output_location(output_path)

    script_call = code

    prepare_qsub_file(qsub_output_path, script_call,
                      job_name, logfile, errfile, env)

    submit_job(qsub_output_path)


def submit_python_script(script_file_path, output_path, script_arguments='', job_name="job", logfile="$PBS_JOBID.output", errfile="$PBS_JOBID.error", env='base'):

    qsub_output_path = prepare_output_location(output_path)

    job_script_name = qsub_output_path / 'job_script.py'

    if not job_script_name.exists():
        shutil.copy(script_file_path, job_script_name)

    script_call = "python " + str(job_script_name) + ' ' + script_arguments

    prepare_qsub_file(qsub_output_path, script_call,
                      job_name, logfile, errfile, env)

    submit_job(qsub_output_path)
