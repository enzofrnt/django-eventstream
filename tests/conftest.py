import pytest
from django.conf import settings

def pytest_configure():
    if not settings.configured:
        settings.configure(
            DATABASES={
                'default': {
                    'ENGINE': 'django.db.backends.sqlite3',
                    'NAME': ':memory:',
                }
            },
            INSTALLED_APPS=['django_eventstream'],
            MIDDLEWARE=[],
            ROOT_URLCONF='tests.urls',
            SECRET_KEY='test-key',
            EVENTSTREAM_REDIS={
                'host': 'localhost',
                'port': 6379,
                'db': 0
            },
            EVENTSTREAM_LISTENER_CLASS='django_eventstream.listeners.RedisListener'
        ) 