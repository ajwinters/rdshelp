from setuptools import setup, find_packages

setup(
    name="rdshelp",
    version="1.0.0",
    packages=find_packages(),
    py_modules=["rdshelp"],  # Name of your Python file
    install_requires=["psycopg2","psycopg2-binary","numpy","pandas"],  # Add any dependencies here if needed
    description="AWS RDS utility functions for database and table creation through panda dataframes",
    author="Alex Winters",
    author_email="awin117@gmail.com",
    license='MIT',
    url="https://github.com/ajwinters/rdshelp",  # Optional, if you have a repo
)


