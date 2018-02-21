try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

from distutils.command.install import install
import os

here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README.md')) as f:
        README = f.read()
    with open(os.path.join(here, 'CHANGES.txt')) as f:
        CHANGES = f.read()
except UnicodeDecodeError:
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        README = f.read()
    with open(os.path.join(here, 'CHANGES.txt'), encoding='utf-8') as f:
        CHANGES = f.read()



config = {
    'description': 'mesoshttp',
    'author': 'Olivier Sallou',
    'author_email': 'olivier.sallou@irisa.fr',
    'download_url': 'https://github.com/osallou/python-mesos-http',
    'version': '0.2.12',
     'classifiers': [
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4'
    ],
    'install_requires': [
                         'requests',
                         'twitter.common.recordio',
                         'kazoo'
                         ],
    'tests_require': ['nose', 'mock', 'flake8'],
    'test_suite': 'nose.collector',
    'packages': find_packages(),
    'include_package_data': True,
    'scripts': [],
    'name': 'mesoshttp',
}

setup(**config)
