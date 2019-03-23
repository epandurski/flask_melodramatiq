"""
Flask-Melodramatiq
------------------

A Flask extension that adds support for the "dramatiq" task processing library.
"""

import sys
from setuptools import setup

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


setup(
    name='Flask-Melodramatiq',
    version='0.3.1',
    url='https://github.com/epandurski/flask_melodramatiq',
    license='MIT',
    author='Evgeni Pandurski',
    author_email='epandurski@gmail.com',
    description='A Flask extension that adds support for the "dramatiq" task processing library',
    long_description=__doc__,
    packages=['flask_melodramatiq'],
    zip_safe=True,
    platforms='any',
    setup_requires=pytest_runner,
    install_requires=[
        'Flask>=1.0',
        'dramatiq>=1.5',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'mock',
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
