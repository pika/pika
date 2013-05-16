# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
from setuptools import setup
import os
import platform

# Conditionally include additional modules for docs
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
requirements = list()
if on_rtd:
    requirements.append('tornado')
    requirements.append('twisted')

# Conditional include unittest2 for versions of python < 2.7
tests_require = ['nose', 'mock', 'pyyaml']
platform_version = list(platform.python_version_tuple())[0:2]
if platform_version[0] != '3' and platform_version != ['2', '7']:
    tests_require.append('unittest2')

long_description = ('Pika is a pure-Python implementation of the AMQP 0-9-1 '
                    'protocol that tries to stay fairly independent of the '
                    'underlying network support  library. Pika was developed '
                    'primarily for use with RabbitMQ, but should also work '
                    'with other AMQP 0-9-1 brokers.')

setup(name='pika',
      version='0.9.13',
      description='Pika Python AMQP Client Library',
      long_description=long_description,
      maintainer='Gavin M. Roy',
      maintainer_email='gavinmroy@gmail.com',
      url='https://pika.readthedocs.org ',
      packages=['pika', 'pika.adapters'],
      license='MPL v1.1 and GPL v2.0 or newer',
      install_requires=requirements,
      extras_require={'tornado': ['tornado'],
                      'twisted': ['twisted']},
      tests_require=tests_require,
      test_suite='nose.collector',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'License :: OSI Approved :: Mozilla Public License 1.1 (MPL 1.1)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Topic :: Communications',
          'Topic :: Internet',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Networking'],
      zip_safe=True)
