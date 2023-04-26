from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    readme = f.read()

setup(
    name='pyserve3',
    version='0.0.5',
    license='MIT',
    packages=find_packages(),
    url='https://github.com/imgurbot12/pyserve',
    author='Andrew Scott',
    author_email='imgurbot12@gmail.com',
    description='Unify SocketServer Implementations based on a Session Model',
    python_requires='>=3.7',
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
