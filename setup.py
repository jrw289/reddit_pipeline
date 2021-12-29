import setuptools


setuptools.setup(
    name="reddit_pipeline",
    version="0.0.1",
    author="James Welch",
    author_email="jrw289@cornell.edu",
    description="A package for pulling data down from Reddit, transform it, and put it into a database using Python modules",
    url="https://github.com/jrw289/reddit-pipeline",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
