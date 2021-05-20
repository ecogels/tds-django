# SQL Server backend for Django 3.2

- django 3.2
- tested and used with SQL Server 2017

## Requirements
- [python-tds](https://github.com/denisenkom/pytds)

- optional:
  - bitarray, recommended by python-tds for performance
  - for regex support you need to compile `clr/django_clr.cs` and install the resulting assembly or read and then run the `tds_django/sql/clr.sql` script.
  - for date "math" as well as bitshift operations you need to read and run the `tds_django/sql/init.sql` script.
    
## Unsupported
- JSON
- foreign keys to a nullable field (limitation of SQL Server)
- feel free to read `tds_django/features.py` for more details.

## Warning If you have used another backend before
- this one uses `uniqueidentifier` field for UUIDField while others may have used nvarchar.

# Installation
`pip install bitarray python-tds tds_django`

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
