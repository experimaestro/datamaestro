#!/usr/bin/env python

import sys

try:
    from setuptools import setup, find_namespace_packages
except ImportError:
    print("Please upgrade pip: find_namesspace_packages not found")
    sys.exit(1)

from setuptools.command.install import install
from pathlib import Path
import re

VERSION='0.5.0'

RE_BLANCK=re.compile(r"^\s*(#.*)?$")
with (Path(__file__).parent / 'requirements.txt').open() as f:
    requirements = [x for x in f.read().splitlines() if not RE_BLANCK.match(x)]

class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='datamaestro',
    version=VERSION,
    description='Dataset management',
    author='Benjamin Piwowarski',
    author_email='benjamin@piwowarski.fr',
    url='https://github.com/bpiwowar/datamaestro',
    packages=find_namespace_packages(include="datamaestro.*"),
    install_requires = requirements,
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_data={'datamaestro': ['LICENSE']},
    data_files=[('config', ['requirements.txt'])],
    cmdclass={
        'verify': VerifyVersionCommand,
    },
    python_requires='>=3',
    entry_points = {
        'console_scripts': [
            'datamaestro = datamaestro.__main__:main',                  
        ],         
        'mkdocs.plugins': [
                'datamaestro = datamaestro.commands.site:DatasetGenerator',
        ]
    },

    test_suite='datamaestro.test'
)
