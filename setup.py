from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()


with open('VERSION', 'r') as version_file:
    version = version_file.readline()

setup(
    name='partitioning-service',
    version=version,
    packages=['app'],
    keywords='hdfs partitioning service impala hive spark api fastapi',
    url='https://git.rz.adition.net/reporting/partitioning-service',
    author='ADITION technologies AG',
    author_email='bi@adition.com',
    description='The partitioning service generates queries WHERE-clauses for hdfs partitioning.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={'': 'app'}
)
