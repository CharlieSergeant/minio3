from setuptools import setup, find_packages

setup(
    name='minio3',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'awswrangler',
        'boto3',
        'botocore',
        'pandas==1.3.4',
        'protobuf==3.19.4',
        'pyarrow==7.0.0',
        'pydantic==1.10.8',
        'python-dotenv==1.0.0',
        'python_dateutil==2.8.1',
        'Requests==2.31.0'
    ],
    entry_points={
        'console_scripts': [

        ],
    },
    author='Charlie Sergeant',
    author_email='sergeach@kean.edu',
    description='',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/CharlieSergeant/minio3',
    license='MIT',  # Choose an appropriate license for your package
)