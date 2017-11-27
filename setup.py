from setuptools import setup
import os

# Conditionally include additional modules for docs
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
requirements = list()
if on_rtd:
    requirements.append('tornado')
    requirements.append('twisted')
    #requirements.append('pyev')

long_description = ('Pika is a pure-Python implementation of the AMQP 0-9-1 '
                    'protocol that tries to stay fairly independent of the '
                    'underlying network support  library. Pika was developed '
                    'primarily for use with RabbitMQ, but should also work '
                    'with other AMQP 0-9-1 brokers.')

setup(name='pika',
      version='0.11.1',
      description='Pika Python AMQP Client Library',
      long_description=open('README.rst').read(),
      maintainer='Gavin M. Roy',
      maintainer_email='gavinmroy@gmail.com',
      url='https://pika.readthedocs.io',
      packages=['pika', 'pika.adapters'],
      license='BSD',
      install_requires=requirements,
      package_data={'': ['LICENSE', 'README.rst']},
      extras_require={'tornado': ['tornado'],
                      'twisted': ['twisted'],
                      'libev': ['pyev']},
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: Jython',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Topic :: Communications',
          'Topic :: Internet',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Networking'],
      zip_safe=True)
