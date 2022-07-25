
SECRET_KEY = "django_tests_secret_key"
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://redis:6379/1",
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    },
    "extended": {
        "BACKEND": "core.django.redis.cache.RedisCacheExtended",
        "LOCATION": "redis://redis:6379/1",
        "OPTIONS": {"CLIENT_CLASS": "core.django.redis.client.DjangoRedisExtended"},
    },
}

# Include `django.contrib.auth` and `django.contrib.contenttypes` for mypy /
# django-stubs.

# See:
# - https://github.com/typeddjango/django-stubs/issues/318
# - https://github.com/typeddjango/django-stubs/issues/534
INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
]

USE_TZ = False