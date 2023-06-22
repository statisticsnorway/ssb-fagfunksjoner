#!/usr/bin/env python
"""Trying to work around a bug which is stopping Pypi publishing. 
https://github.com/pypa/gh-action-pypi-publish/issues/162#issuecomment-1600758192
Would prefer working in pyproject.toml instead, but trying this"""

from distutils.core import setup

setup(
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)