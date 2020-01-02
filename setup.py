# Copyright 2019 Typo. All Rights Reserved.
#
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
#
# you may not use this file except in compliance with the
#
# License.
#
#
#
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#
#
# Unless required by applicable law or agreed to in writing, software
#
# distributed under the License is distributed on an "AS IS" BASIS,
#
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#
# implied. See the License for the specific language governing
#
# permissions and limitations under the License.
#
#
#
# This product includes software developed at
#
# or by Typo (https://www.typo.ai/).
# !/usr/bin/env python
from setuptools import setup, find_packages
from os import path


def read(*paths):
    filename = path.join(path.abspath(path.dirname(__file__)), *paths)
    with open(filename) as f:
        return f.read()


setup(
    name='target-typo-proxy',
    version='0.1.0',
    description=(
        'Typo is the intelligent data quality barrier for '
        'enterprise information systems. The Typo target proxy '
        'communicates with Singer taps, consumes data that '
        'conforms to the Singer JSON specification, and '
		'provides data quality services to data in motion.'
    ),
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Typo',
    license='Apache 2.0',
    keywords='typo.ai data quality singer target',
    url='https://www.typo.ai/',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
    py_modules=['target_typo_proxy'],
    packages=find_packages(),
    install_requires=[
        'singer-python>=5.0.12',
        'requests>=2.21.0',
        'jsonschema>=2.6.0,<3.0a',
    ],
    entry_points={
        'console_scripts': [
            'target-typo-proxy=target_typo_proxy:main',
        ],
    }
)
