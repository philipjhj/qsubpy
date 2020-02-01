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


HPC_PLATFORM = 'torque'

LOG_PATH = 'logs'

HPC_FILE_NAME = None
TEMPLATE_BASE = None
TEMPLATE_HPC_CODE = None
PYTHON_CLEANUP_CODE_TEMPLATE = None


def config(hpc_platform='torque'):
    global HPC_PLATFORM, HPC_FILE_NAME, TEMPLATE_BASE, TEMPLATE_HPC_CODE, PYTHON_CLEANUP_CODE_TEMPLATE

    HPC_PLATFORM = hpc_platform

    if HPC_PLATFORM == 'torque':

        HPC_FILE_NAME = 'hpc_file.qsub'

        TEMPLATE_BASE = """
        #!/bin/sh
        # embedded options to qsub - start with #PBS
        # -- Name of the job ---
        #PBS -N {job_name}
        # -- specify queue --
        #PBS -q hpc
        # -- estimated wall clock time (execution time): hh:mm:ss --
        #PBS -l walltime={walltime}
        # -- number of processors/cores/nodes --
        #PBS -l nodes={n_nodes}:ppn={ppn}
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

        TEMPLATE_HPC_CODE = TEMPLATE_BASE+"""
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
    elif HPC_PLATFORM == 'lsf':
        HPC_FILE_NAME = 'hpc_file.bsub'

        TEMPLATE_BASE = """
        #!/bin/sh
        # embedded options to bsub
        # -- Name of the job ---
        #BSUB -J {job_name}
        # -- specify queue --
        #BSUB -q hpc
        # -- estimated wall clock time (execution time): hh:mm:ss --
        #BSUB -W {walltime}
        # -- number of processors/cores/nodes --
        #BSUB -n {ppn}
        #BSUB -R "span[ptile={n_nodes}]"
        # -- user email address --
        # please uncomment the following line and put in your e-mail address,
        # if you want to receive e-mail notifications on a non-default address
        ##BSUB -u your_email_address
        # -- mail notification --
        ##BSUB -BN 
        #BSUB -o {logfile}
        #BSUB -e {errfile}

        # here follow the commands you want to execute

        echo '**** Activating conda environment ****: env_name = '{env}
        source activate {env}
        """

        TEMPLATE_HPC_CODE = TEMPLATE_BASE+"""
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
    hpc_output_path = Path(output_path) / 'hpc_files'
    hpc_output_path.mkdir(parents=True, exist_ok=True)
    hpc_output_path.joinpath(LOG_PATH).mkdir(parents=True, exist_ok=True)

    return hpc_output_path


def prepare_script_from_code(code, code_cleanup_template, script_file):
    script_file = str(script_file)

    script_text = code + \
        code_cleanup_template.format(
            script=script_file)

    open(script_file, 'w').write(script_text)


def prepare_hpc_file(hpc_output_path, script_call, job_name, logfile, errfile, env, n_nodes=1, ppn=1, walltime='10:00:00'):
    if HPC_PLATFORM == 'lsf':
        # lsf takes total number of cores
        ppn = ppn*n_nodes

    with open(str(hpc_output_path / HPC_FILE_NAME), 'w') as f:
        f.write(TEMPLATE_HPC_CODE.format(script_call=script_call,
                                         job_name=job_name,
                                         logfile=hpc_output_path/LOG_PATH/logfile,
                                         errfile=hpc_output_path/LOG_PATH/errfile,
                                         env=env,
                                         n_nodes=n_nodes,
                                         ppn=ppn,
                                         walltime=walltime))


def submit_job(hpc_output_path):
    if HPC_PLATFORM == 'torque':
        subprocess.call('qsub < ' + str(hpc_output_path /
                                        HPC_FILE_NAME), shell=True)
    elif HPC_PLATFORM == 'lsf':
        subprocess.call('bsub < ' + str(hpc_output_path /
                                        HPC_FILE_NAME), shell=True)


def submit_python_code(code, output_path, script_arguments='', job_name="job", logfile="$PBS_JOBID.output", errfile="$PBS_JOBID.error", env='base', n_nodes=1, ppn=1, walltime='10:00:00'):

    hpc_output_path = prepare_output_location(output_path)

    script_file_name = hpc_output_path / 'job_script.py'

    prepare_script_from_code(
        code, PYTHON_CLEANUP_CODE_TEMPLATE, script_file_name)

    submit_python_script(script_file_name, output_path, script_arguments=script_arguments,
                         job_name=job_name, logfile=logfile, errfile=errfile, env=env,
                         n_nodes=n_nodes, ppn=ppn, walltime=walltime)


def submit_bash_code(code, output_path, job_name="job", logfile="$PBS_JOBID.output", errfile="$PBS_JOBID.error", env='base', n_nodes=1, ppn=1, walltime='10:00:00'):

    hpc_output_path = prepare_output_location(output_path)

    script_call = code

    prepare_hpc_file(hpc_output_path, script_call,
                     job_name, logfile, errfile, env,
                     n_nodes, ppn, walltime)

    submit_job(hpc_output_path)


def submit_python_script(script_file_path, output_path, script_arguments='', job_name="job", logfile="$PBS_JOBID.output", errfile="$PBS_JOBID.error", env='base', n_nodes=1, ppn=1, walltime='10:00:00'):

    hpc_output_path = prepare_output_location(output_path)

    job_script_name = hpc_output_path / 'job_script.py'

    if not job_script_name.exists():
        shutil.copy(script_file_path, job_script_name)

    script_call = "python " + str(job_script_name) + ' ' + script_arguments

    prepare_hpc_file(hpc_output_path, script_call,
                     job_name, logfile, errfile, env,
                     n_nodes, ppn, walltime)

    submit_job(hpc_output_path)
