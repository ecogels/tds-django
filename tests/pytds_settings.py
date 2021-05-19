
params = {
    'ENGINE': 'tds_django',
    'NAME': 'django_tests',
    'USER': 'sa',
    'PASSWORD': 'P4ssword',
}

DATABASES = {
    'default': params,
    'other': {
        **params,
        'NAME': 'django_other',
    },
}

SECRET_KEY = "django_tests_secret_key"

PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.MD5PasswordHasher',
]

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
