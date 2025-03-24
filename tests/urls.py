import django_eventstream
from django.urls import path, include
from django_eventstream.views import events
urlpatterns = [
    path('events/', events, {"channels": ["test"]}, name='events'),
    # path("events/", include(django_eventstream.urls), {"channels": ["test"]}, name="events"),
] 