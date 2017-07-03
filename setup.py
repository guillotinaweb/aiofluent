#!/usr/bin/python

from os import path

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

README = path.abspath(path.join(path.dirname(__file__), 'README.rst'))
desc = 'A Python logging handler for Fluentd event collector'

setup(
  name='aiofluent',
  version='1.0.0',
  description=desc,
  long_description=open(README).read(),
  package_dir={'fluent': 'fluent'},
  packages=['fluent'],
  install_requires=['msgpack-python'],
  author='Nathan Van Gheem',
  author_email='vangheem@gmail.com',
  url='https://github.com/vangheem/aiofluent',
  download_url='http://pypi.python.org/pypi/aiofluent/',
  license='Apache License, Version 2.0',
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
  ],
  test_suite='tests'
)
