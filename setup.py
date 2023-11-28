from setuptools import setup, find_packages

VERSION = '0.1.0'
DESCRIPTION = 'Joey'
LONG_DESCRIPTION = """
Event analytics. Very fast. Will eventually be merged into Quokka
"""

# Setting up
setup(

        name="pyjoey",
        version=VERSION,
        author="Tony Wang",
        author_email="zihengw@stanford.edu",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[
            'pyarrow>=7.0.0',
            'sqlglot>=17.8.0',
            'tqdm>=4.65.0',
            'polars>=0.18.0', # latest version of Polars generally
            ], # add any additional packages that 
)

