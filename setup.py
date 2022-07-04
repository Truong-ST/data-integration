from setuptools import setup, find_packages
from vimap import VERSION
import os


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths


# extra_files = package_files("vimap/cli/default_example")


with open('requirements.txt') as f:
    required_packages = f.readlines()


setup(
    name='vimap',
    version=VERSION,
    description="Vimap is ETL to build data warehouse for place data",
    author="tungnk",
    author_email="nguyenkytungcntt04@gmail.com",
    keywords="vimap",
    packages=find_packages(),
    include_package_data=True,
    py_modules=['vimap'],
    install_requires=required_packages,
    python_requires='>3.6.0',
    # package_data={
    #     "vimap": extra_files
    # },
    entry_points={
        'console_scripts': [
            'vimap = vimap.run_cli:entry_point'
        ]
    },
)