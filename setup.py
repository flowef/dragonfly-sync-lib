import setuptools

with open("README.md") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dragonfly-sync-lib",
    version="0.1.0",
    author="Stephan Chang",
    author_email="stephan.chang@flowef.com",
    description="A library that facilitates transporting data around.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flowef/dragonfly-sync-lib",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent"
    ])
