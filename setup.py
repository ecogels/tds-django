from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="tds-django",
    version="0.0.1",
    author="Etienne Cogels",
    author_email="ecogels@users.noreply.github.com",
    description="Django backend for SQL Server using tds",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ecogels/tds-django",
    project_urls={
        "Bug Tracker": "https://github.com/ecogels/tds-django/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 1 - Planning",
        "Framework :: Django",
        "Framework :: Django :: 3.2",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
    packages=find_packages(include=['tds_django']),
    python_requires=">=3.6",
)
