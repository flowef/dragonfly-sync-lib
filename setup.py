import setuptools

with open("README.md") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dragonfly-sync",
    version="0.1.0",
    author="Stephan Chang",
    author_email="stchepanhagn@gmail.com",
    description="A library that facilitates transporting data around",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stchepanhagn/dragonfly",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent"
    ])
