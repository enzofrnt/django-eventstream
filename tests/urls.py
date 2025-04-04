from django.urls import path

from django_eventstream.views import events

from .views import send_event_view

urlpatterns = [
    path("events/", events, {"channels": ["test"]}, name="events"),
    path("send_event/", send_event_view, name="send_event"),
]
