from setuptools import setup, find_packages


setup(
    name='tiny_data_server',
    version='1.0',
    author='tinybird.co',
    author_email='jobs@tinybird.co',
    description='A Tiny HTTP data handler server',
    packages=find_packages(),
    install_requires=[
        'tornado==6.1',
    ],
    entry_points={
        'console_scripts': [
            'tiny_data_server=app:run',
        ],
    }
)
