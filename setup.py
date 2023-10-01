"""Setup for packaging"""
import io
import os
import sys
from setuptools import find_packages, setup, Command
from shutil import rmtree


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

# Package meta-data.
NAME = 'omop2owl-vocab'
DESCRIPTION = 'Convert OMOP vocab into OWL and SemanticSQL.'
URL = 'https://github.com/HOT-Ecosystem/omop2owl-vocab/'
EMAIL = 'jflack@jhu.edu'
AUTHOR = 'Joe Flack'
REQUIRES_PYTHON = '>=3.9.0'
VERSION = '1.1.0'

# Requirements
REQUIRED = [
    'oaklib',
    'pandas',
]

# Description
with io.open(os.path.join(PROJECT_ROOT, 'README.md'), encoding='utf-8') as f:
    long_description = '\n' + f.read()


# As of 3.5 I believe, this doesn't work. Use `make pypi` and `make pypi-test`, which use twine.
class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        """Init options"""
        pass

    def finalize_options(self):
        """Finalize options"""
        pass

    def run(self):
        """Run upload"""
        self.status('Removing previous builds…')
        dist_dir = os.path.join(PROJECT_ROOT, 'dist')
        if os.path.exists(dist_dir):
            rmtree(dist_dir)
        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))
        self.status('Uploading the package to PyPI via Twine…')
        os.system('twine upload dist/*')
        self.status('Pushing git tags…')
        os.system('git tag v{0}'.format(VERSION))
        os.system('git push --tags')
        sys.exit()


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('test',)),
    package_data={
        'omop2owl_vocab': [
            'omop2owl_vocab/prefixes.csv',
        ]
    },
    install_requires=REQUIRED,
    # extras_require=EXTRAS,
    include_package_data=True,
    # license='MIT',  # todo: add LICENSE.md from GitHub and add license
    classifiers=[
        # todo: does this really qualify?
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    # $ setup.py publish support.
    cmdclass={
        'upload': UploadCommand,
    },
    entry_points={
        'console_scripts': [
            'omop2owl-vocab = omop2owl_vocab.__main__:cli'
        ]
    },
)
