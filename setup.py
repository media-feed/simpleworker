from setuptools import find_packages, setup


with open('README.rst', 'rb') as f:
    long_description = f.read().decode('utf-8')


setup(
    name='SimpleWorker',
    version='0.0.dev0',
    packages=find_packages(),

    author='Eduardo Klosowski',
    author_email='eduardo_klosowski@yahoo.com',

    description='Fila de trabalho',
    long_description=long_description,
    license='MIT',
    url='https://github.com/media-feed/simpleworker',
)
