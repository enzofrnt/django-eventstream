"""
ASGI config for mon_projet project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
# print("COUCOUCOUCOUCOU \nsettings.EVENTSTREAM_LISTENER_CLASS")
# from django.conf import settings

# if hasattr(settings, "EVENTSTREAM_LISTENER_CLASS"):
#     print(settings.EVENTSTREAM_LISTENER_CLASS)
# else:
#     print("NOPE")

application = get_asgi_application()
