from setuptools import setup, find_packages


setup(
    name="reddit_service_activity",
    packages=find_packages(exclude="tests"),
    install_requires=[
        "baseplate",
        "thrift",
        "pyramid",
        "redis",
    ],
    tests_require=[
        "nose",
        "coverage",
        "mock",
    ],
)
