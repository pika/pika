# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
from setuptools import setup

import platform

# Conditional include unittest2 for versions of python < 2.7
tests_require=['nose', 'mock']
platform_version = list(platform.python_version_tuple())[0:2]
if platform_version[0] != '3' and platform_version != ['2', '7']:
    tests_require.append('unittest2')

long_description = ('Pika is a pure-Python implementation of the AMQP 0-9-1 '
                    'protocol that tries to stay fairly independent of the '
                    'underlying network support  library. Pika was developed '
                    'primarily for use with RabbitMQ, but should also work '
                    'with other AMQP 0-9-1 brokers.')

setup(name='pika',
      version='0.9.6-pre1',
      description='Pika Python AMQP Client Library',
      long_description=long_description,
      author='Tony Garnock-Jones',
      author_email='tonygarnockjones@gmail.com',
      maintainer='Gavin M. Roy',
      maintainer_email='gmr@meetme.com',
      url='https://github.com/pika ',
      packages=['pika', 'pika.adapters'],
      license='MPL v1.1 and GPL v2.0 or newer',
      tests_require=tests_require,
      test_suite = "nose.collector",
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'License :: OSI Approved :: Mozilla Public License 1.1 (MPL 1.1)',
        'Operating System :: OS Independent',
        'Topic :: Communications',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries',
        ],
        zip_safe=True
      )
