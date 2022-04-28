from setuptools import setup


with open("README.md") as f:
    long_desc = f.read()

setup(
    name="SQuidS",
    version='0.1.0',
    author='Ryan Siemens',
    author_email="ryanjsiemens@gmail.com",
    description="SQuidS is a Python task library for AWS SQS",
    license="MIT License",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    url="https://github.com/rsiemens/squids/",
    packages=["squids"],
    install_requires=["boto3>=1.0.0,<2.0.0"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
