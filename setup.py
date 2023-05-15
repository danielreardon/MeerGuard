#! /usr/bin/env python
"""
Setup for meerguard/coastguard
"""
from setuptools import setup, find_packages

reqs = [
    'numpy',
    'scipy',
    'matplotlib',
]

setup(
    name="coast_guard",
    description="MeerTime copy of coast_guard: Stripped for RFI excision and modified for zapping of wide-bandwidth data with a frequency-dependent template",
    url="https://github.com/danielreardon/MeerGuard",
    install_requires=reqs,
    packages=find_packages(),
    package_data={'coast_guard':['configurations/*.cfg', 'configurations/receivers/*.cfg']},
    python_requires='>=3.7',
    scripts=[
        "clean_archive.py",
    ],
)