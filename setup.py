#!/usr/bin/env python

from distutils.core import setup

setup(name='datasets',
    version='0.1',
    description='Datasets utilities',
    author='Benjamin Piwowarski',
    author_email='benjamin@bpiwowar.net',
    url='https://github.com/bpiwowar/datasets',
    packages=['datasets'],
    package_dir={'datasets': 'datasets'},
    install_requires = [ 'Click' ],
    package_data={'datasets': ['LICENSE', 'datasets/repositories.yaml']},
    data_files = [
        
    ]
    entry_points = {
        'console_scripts': [
            'datasets = datasets.__main__:main',                  
        ],         
        'mkdocs.plugins': [
                'datasets = datasets.commands.site:DatasetGenerator',
        ]
    },
)
