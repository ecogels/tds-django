# SQL Server backend for Django >=3.2
- django 4
- django 3.2 (pypi package version 0.1)
- tested and used with SQL Server 2017 and for version 4.2 with python 3.11 only

## Warning
- There is an official package supported by microsoft, microsoft/mssql-django.
- This package does not need pyodbc nor the microsoft odbc driver, only pytds.
- This passes about 15000 tests from the django test suite, but I personally use the django ORM in a basic way and
don't use most of the features.

## Requirements
- [python-tds](https://github.com/denisenkom/pytds)

- optional:
  - bitarray, recommended by python-tds for performance
  - for regex support you need to compile `clr/django_clr.cs` and install the resulting assembly or read and then run
the `tds_django/sql/clr.sql` script.
  - for date "math" as well as bit-shift operations you need to read and run the `tds_django/sql/init.sql` script.
    
## Unsupported
- JSON
- foreign keys to a nullable field (limitation of SQL Server)
- feel free to read `tds_django/features.py` for more details.
- queryset iterator with chunk size

## Warning If you have used another backend before
- this one uses `uniqueidentifier` field for UUIDField while others may have used nvarchar.

# Installation
For django 4.2
`pip install bitarray python-tds tds_django==4.2.0`

For django 4.1
`pip install bitarray python-tds tds_django==4.1.0`

For django 4.0
`pip install bitarray python-tds tds_django==4.0.0`

For django 3.2
`pip install bitarray python-tds tds_django==0.1`

# settings.DATABASES

```python
DATABASES = {
    'default': {
        'ENGINE': 'tds_django',
        'HOST': 'localhost',
        'PORT': '1433',
        'NAME': '<db_name>',
        'USER': '<db_user>',
        'PASSWORD': '<db_password>',
    }, 
}
```
