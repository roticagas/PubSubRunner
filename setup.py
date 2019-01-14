import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="PubSubRunner",
    version="1.1.6",
    author="Chettha Thawornsathit",
    author_email="roticagas@gmail.com",
    description="Python Boilerplate for Google Cloud PubSub running task in python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/roticagas/PubSubRunner",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)