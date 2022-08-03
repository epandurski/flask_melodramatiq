"""
Flask-Melodramatiq
------------------

A Flask extension that adds support for the "dramatiq" task processing library.
"""

import os
import sys
from setuptools import setup

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


def rel(*xs):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *xs)


with open(rel("README.rst")) as f:
    long_description = f.read()

setup(
    name='Flask-Melodramatiq',
    version='1.0',
    url='https://github.com/epandurski/flask_melodramatiq',
    license='MIT',
    author='Evgeni Pandurski',
    author_email='epandurski@gmail.com',
    description='A Flask extension that adds support for the "dramatiq" task processing library',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    packages=['flask_melodramatiq'],
    zip_safe=True,
    platforms='any',
    setup_requires=pytest_runner,
    install_requires=[
        'Flask>=1.0',
        'dramatiq>=1.5',
    ],
    tests_require=[
        'pytest~=6.2',
        'pytest-cov~=2.7',
        'mock~=2.0',
        'pika>=0.13',
        'redis>=3.4',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    project_urls={
        "Bug Tracker": "https://github.com/epandurski/flask_melodramatiq/issues",
        "Documentation": "https://flask-melodramatiq.readthedocs.io/en/latest/",
        "Source Code": "https://github.com/epandurski/flask_melodramatiq",
    }
)
