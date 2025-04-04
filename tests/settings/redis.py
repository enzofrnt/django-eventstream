from .base import *

EVENTSTREAM_LISTENER_CLASS = "django_eventstream.listeners.RedisListener"
EVENTSTREAM_REDIS = {"host": "localhost", "port": 6379, "db": 0}
