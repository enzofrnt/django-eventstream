import json

from django.http import HttpResponse

from django_eventstream import send_event


def send_event_view(request):
    try:
        data = json.loads(request.body)
        channel = data.get("channel")
        event_type = data.get("event_type")
        data = data.get("data")
        send_event(channel, event_type, data)
        return HttpResponse("Event sent")
    except json.JSONDecodeError:
        return HttpResponse("Invalid JSON body", status=400)
