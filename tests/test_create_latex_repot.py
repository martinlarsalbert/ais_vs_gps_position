from src.notebook_to_latex import convert_notebook_to_latex
import pytest
import os
import reports
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

@pytest.fixture
def build_directory(tmpdir):

    directory = os.path.join(str(tmpdir),'report_latex')

    if not os.path.exists(directory):
        os.mkdir(directory)

    yield directory

@pytest.fixture
def notebook_path():
    report_dir = os.path.join(reports.path,'report')
    os.chdir(report_dir)
    yield os.path.join(report_dir, '01.1.report.ipynb')


def test_create_latex_report(notebook_path, build_directory):

    convert_notebook_to_latex(notebook_path=notebook_path, build_directory=build_directory,save_main=True, skip_figures=False)

def test_run_notebook(notebook_path):

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
    ep.preprocess(nb)
