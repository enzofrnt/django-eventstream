# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import asyncio
import copy
import logging
import threading
from asgiref.sync import sync_to_async
from django.http import HttpResponseBadRequest, StreamingHttpResponse
from .utils import add_default_headers
from django.conf import settings
from .listeners import FileSystemListener
from .event import Event

logger = logging.getLogger(__name__)

MAX_PENDING = 10


class SSEClient(object):
    def __init__(self):
        self.loop = None
        self.aevent = asyncio.Event()
        self.user_id = ""
        self.channels = set()
        self.channel_items = {}
        self.overflow = False
        self.error = ""
        # Récupérer le listener approprié
        self.event_listener = get_listener_manager().get_event_listener()
        logger.info(f"SSEClient initialisé avec l'ID {id(self)}")

    def assign_loop(self):
        self.loop = asyncio.get_event_loop()
        logger.info(f"SSEClient {id(self)}: Loop assignée")

    def wake_threadsafe(self):
        logger.info(f"SSEClient {id(self)}: Réveil du client")
        self.loop.call_soon_threadsafe(self.aevent.set)

    async def process_event(self, channel, event_type, data):
        """Traite un événement reçu."""
        logger.info(f"SSEClient {id(self)}.process_event: Traitement de l'événement {event_type} sur le canal {channel}")
        e = Event(channel, event_type, data)
        items = self.channel_items.get(channel)
        if items is None:
            items = []
            self.channel_items[channel] = items
        if len(items) < MAX_PENDING:
            items.append(e)
            logger.info(f"SSEClient {id(self)}.process_event: Événement ajouté à la file d'attente")
            self.wake_threadsafe()
        else:
            logger.warning(f"SSEClient {id(self)}.process_event: File d'attente pleine, overflow activé")
            self.overflow = True


class ListenerManager(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.listeners_by_channel = {}
        self.event_listener = None
        self.listener_started = False
        self.sse_clients = set()  # Ensemble des clients SSE connectés
        logger.info("ListenerManager initialisé")
        # Initialiser le listener système au démarrage
        self.event_listener = self.get_event_listener()
        logger.info("ListenerManager: Listener système initialisé")

    def get_event_listener(self):
        """Sélectionne le listener approprié en fonction de la configuration."""
        if hasattr(settings, "EVENTSTREAM_LISTENER_CLASS"):
            try:
                # Import dynamique de la classe à partir du chemin complet
                module_path, class_name = settings.EVENTSTREAM_LISTENER_CLASS.rsplit('.', 1)
                module = __import__(module_path, fromlist=[class_name])
                listener_class = getattr(module, class_name)
                logger.info(f"ListenerManager: Utilisation du listener configuré: {class_name}")
                return listener_class()
            except (ImportError, AttributeError) as e:
                logger.error(f"ListenerManager: Erreur lors de l'import du listener: {e} \n Utilisation du listener par défaut")
                return FileSystemListener()
        logger.info("ListenerManager: Utilisation du FileSystemListener par défaut")
        return FileSystemListener()

    async def start_listener(self):
        """Démarre le listener sélectionné."""
        if self.event_listener and not self.listener_started:
            logger.info("ListenerManager: Démarrage du listener système")
            await self.event_listener.start()
            self.listener_started = True
            logger.info("ListenerManager: Listener système démarré")

    def add_sse_client(self, client):
        """Ajoute un nouveau client SSE."""
        logger.info(f"ListenerManager: Ajout du client SSE {id(client)}")
        with self.lock:
            self.sse_clients.add(client)
            # Démarrer le listener si ce n'est pas déjà fait
            if not self.listener_started and self.event_listener:
                loop = asyncio.get_event_loop()
                loop.create_task(self.start_listener())

            for channel in client.channels:
                logger.info(f"ListenerManager: Inscription du client {id(client)} au canal {channel}")
                clisteners = self.listeners_by_channel.get(channel)
                if clisteners is None:
                    clisteners = set()
                    self.listeners_by_channel[channel] = clisteners
                clisteners.add(client)

    def remove_sse_client(self, client):
        """Supprime un client SSE."""
        logger.info(f"ListenerManager: Suppression du client SSE {id(client)}")
        with self.lock:
            self.sse_clients.remove(client)
            for channel in client.channels:
                clisteners = self.listeners_by_channel.get(channel)
                clisteners.remove(client)
                if len(clisteners) == 0:
                    del self.listeners_by_channel[channel]
                    logger.info(f"ListenerManager: Suppression du canal {channel} (plus de clients)")

    def send_event(self, channel, event_type, data):
        """Gère l'envoi des événements en fonction du listener utilisé."""
        logger.info(f"ListenerManager.send_event: Envoi de l'événement {event_type} sur le canal {channel}")
        if self.event_listener:
            # Utiliser la même configuration que get_event_listener
            if hasattr(settings, "EVENTSTREAM_LISTENER_CLASS"):
                try:
                    module_path, class_name = settings.EVENTSTREAM_LISTENER_CLASS.rsplit('.', 1)
                    module = __import__(module_path, fromlist=[class_name])
                    listener_class = getattr(module, class_name)
                    
                    # Utiliser la méthode send_event du listener
                    logger.info(f"ListenerManager.send_event: Utilisation du listener {class_name}")
                    self.event_listener.send_event(channel, event_type, data)
                except (ImportError, AttributeError) as e:
                    logger.error(f"ListenerManager.send_event: Erreur lors de l'envoi d'événement: {e}")
                    # Fallback vers le comportement par défaut
                    logger.info("ListenerManager.send_event: Utilisation du FileSystemListener par défaut")
                    self.event_listener.send_event(channel, event_type, data)
            else:
                # Comportement par défaut avec FileSystemListener
                logger.info("ListenerManager.send_event: Utilisation du FileSystemListener par défaut")
                self.event_listener.send_event(channel, event_type, data)
        else:
            # Si aucun listener n'est configuré, envoyer directement aux clients
            logger.info("ListenerManager.send_event: Aucun listener configuré, envoi direct aux clients")
            e = Event(channel, event_type, data)
            self.add_to_queues(channel, e)

    def add_to_queues(self, channel, event):
        """Ajoute un événement aux files d'attente des clients SSE."""
        logger.info(f"ListenerManager.add_to_queues: Ajout de l'événement {event.type} sur le canal {channel}")
        with self.lock:
            wake = []
            clients = self.listeners_by_channel.get(channel, set())
            logger.info(f"ListenerManager.add_to_queues: {len(clients)} clients trouvés pour le canal {channel}")
            for client in clients:
                items = client.channel_items.get(channel)
                if items is None:
                    items = []
                    client.channel_items[channel] = items
                if len(items) < MAX_PENDING:
                    logger.info(f"ListenerManager.add_to_queues: Événement ajouté pour le client {id(client)}")
                    items.append(event)
                    wake.append(client)
                else:
                    logger.warning(f"ListenerManager.add_to_queues: File d'attente pleine pour le client {id(client)}")
                    client.overflow = True
            for client in wake:
                logger.info(f"ListenerManager.add_to_queues: Réveil du client {id(client)}")
                client.wake_threadsafe()

    def kick(self, user_id, channel):
        """Expulse un utilisateur d'un canal."""
        with self.lock:
            wake = []
            clients = self.listeners_by_channel.get(channel, set())
            for client in clients:
                if client.user_id == user_id:
                    logger.info(f"setting error on client {id(client)}")
                    msg = "Permission denied to channels: %s" % channel
                    client.error = {
                        "condition": "forbidden",
                        "text": msg,
                        "extra": {"channels": [channel]},
                    }
                    wake.append(client)
            for client in wake:
                client.wake_threadsafe()


listener_manager = ListenerManager()


def get_listener_manager():
    return listener_manager


async def stream(event_request, client):
    from .eventstream import get_events, EventPermissionError
    from .utils import sse_encode_event, sse_encode_error, make_id

    get_events = sync_to_async(get_events)

    client.assign_loop()

    lm = get_listener_manager()
    lm.add_sse_client(client)

    try:
        first_result = True

        while True:
            try:
                event_response = await get_events(event_request)
            except EventPermissionError as e:
                body = sse_encode_error(
                    "forbidden", str(e), extra={"channels": e.channels}
                )
                yield body
                break

            last_ids = copy.deepcopy(event_response.channel_last_ids)
            event_id = make_id(last_ids)

            body = ""

            if first_result:
                first_result = False

                # include padding on the first result
                body += ":" + (" " * 2048) + "\n\n"
                body += "event: stream-open\ndata:\n\n"

            if len(event_response.channel_reset) > 0:
                body += sse_encode_event(
                    "stream-reset",
                    {"channels": list(event_response.channel_reset)},
                    event_id=event_id,
                    json_encode=True,
                )

            for channel, items in event_response.channel_items.items():
                for item in items:
                    last_ids[channel] = item.id
                    event_id = make_id(last_ids)
                    body += sse_encode_event(item.type, item.data, event_id=event_id)

            yield body

            if len(event_response.channel_more) > 0:
                # read again immediately
                continue

            # FIXME: reconcile without re-reading from db

            lm.lock.acquire()
            conflict = False
            if len(client.channel_items) > 0:
                # items were queued while reading from the db. toss them and
                #   read from db again
                client.aevent.clear()
                client.channel_items = {}
                conflict = True
            lm.lock.release()

            if conflict:
                continue

            # if we get here then the client is caught up. time to wait

            while True:
                f = asyncio.ensure_future(client.aevent.wait())
                while True:
                    done, _ = await asyncio.wait([f], timeout=20)
                    if f in done:
                        break
                    body = "event: keep-alive\ndata:\n\n"
                    yield body

                lm.lock.acquire()

                channel_items = client.channel_items
                overflow = client.overflow
                error_data = client.error

                client.aevent.clear()
                client.channel_items = {}
                client.overflow = False

                lm.lock.release()

                body = ""
                for channel, items in channel_items.items():
                    for item in items:
                        if channel in last_ids:
                            if item.id is not None:
                                last_ids[channel] = item.id
                            else:
                                del last_ids[channel]
                        if last_ids:
                            event_id = make_id(last_ids)
                        else:
                            event_id = None
                        body += sse_encode_event(
                            item.type, item.data, event_id=event_id
                        )

                more = True

                if error_data:
                    condition = error_data["condition"]
                    text = error_data["text"]
                    extra = error_data.get("extra")
                    body += sse_encode_error(condition, text, extra=extra)
                    more = False

                if body or not more:
                    yield body

                if not more:
                    break

                if overflow:
                    # check db
                    break

            event_request.channel_last_ids = last_ids
    finally:
        client.aevent.set()
        lm.remove_sse_client(client)


def events(request, **kwargs):
    from .eventrequest import EventRequest
    from .eventstream import EventPermissionError, get_events
    from .utils import sse_error_response

    try:
        event_request = EventRequest(request, view_kwargs=kwargs)
        response = None
    except EventRequest.ResumeNotAllowedError as e:
        response = HttpResponseBadRequest("Invalid request: %s.\n" % str(e))
    except EventRequest.GripError as e:
        if request.grip.proxied:
            response = sse_error_response("internal-error", "Invalid internal request.")
        else:
            response = sse_error_response(
                "bad-request", "Invalid request: %s." % str(e)
            )
    except EventRequest.Error as e:
        response = sse_error_response("bad-request", "Invalid request: %s." % str(e))
    except EventPermissionError as e:
        response = sse_error_response("forbidden", str(e), {"channels": e.channels})

    # for grip requests, prepare immediate response
    if not response and hasattr(request, "grip") and request.grip.proxied:
        try:
            event_response = get_events(event_request)
            response = event_response.to_grip_response(request)
        except EventPermissionError as e:
            response = sse_error_response("forbidden", str(e), {"channels": e.channels})

    # if this was a grip request or we encountered an error, respond now
    if response:
        add_default_headers(response, request=request)
        return response

    # if we got here then the request was not a grip request, and there
    #   were no errors, so we can begin a local stream response

    # Créer un nouveau client SSE
    client = SSEClient()
    client.user_id = event_request.user.pk if event_request.user else "anonymous"
    client.channels = event_request.channels

    response = StreamingHttpResponse(
        stream(event_request, client), content_type="text/event-stream"
    )
    add_default_headers(response, request=request)

    return response
