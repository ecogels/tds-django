from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="tds-django",
    version="4.2.2",
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
        "Development Status :: 4 - Beta",
        "Framework :: Django",
        "Framework :: Django :: 3.2",
        "Framework :: Django :: 4.0",
        "Framework :: Django :: 4.1",
        "Framework :: Django :: 4.2",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['sql/*.sql']},
    python_requires=">=3.8",
)
