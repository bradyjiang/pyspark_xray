import re
from setuptools import setup, find_packages

def get_version():
    '''Get version without importing, which avoids dependency issues'''
    with open('pyspark_xray/version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')


def readme():
    with open('README.adoc') as f:
        return f.read()


install_requires = []
tests_require = []
setup_requires = []
lint_requires = []
setup(
    name='pyspark_xray',
    version=get_version(),
    author='Brady Jiang',
    author_email='savenowclub@gmail.com',
    url='https://github.com/bradyjiang/pyspark_xray',
    description=('pyspark_xray is a diagnostic tool, in the form of Python library,'
                 ' for pyspark developers to debug and troubleshoot PySpark '
                 'applications locally, specifically it enables '
                 'local debugging of PySpark RDD transformation functions.'),
    long_description=readme(),
    license='Apache License',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
)