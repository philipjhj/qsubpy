from qsubpy import qsub
from pathlib import Path

# TODO: tests should ensure that jobs finish and correct output is generated


def test_simple_submission():
    logpath = 'small_test.log'
    errpath = 'small_test.err'
    output_path = Path('test_output/simple_submission')
    qsub.submit_python_code("print('hi')",
                            output_path,
                            job_name='model',
                            logfile=logpath, errfile=errpath)


def test_default_submission():
    code = "print('hi')"
    output_path = Path('test_output/default_submission')
    qsub.submit_python_code(
        code,
        output_path,
        job_name='test_default_sub',
        env='py37')
