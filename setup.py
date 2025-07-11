from setuptools import find_packages, setup

setup(
    name='flight-data-management',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A project for managing flight data including ETL processes and database management.',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'sqlalchemy',
        'openpyxl',
        'pytest'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.12',
)