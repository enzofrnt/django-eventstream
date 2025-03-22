from abc import ABC, abstractmethod
import asyncio
import json
import logging
from django.conf import settings
from .event import Event

logger = logging.getLogger(__name__)

class BaseEventListener(ABC):
    """Interface de base pour tous les listeners d'événements."""
    
    @abstractmethod
    async def start(self):
        """Démarre le listener."""
        pass

    @abstractmethod
    def stop(self):
        """Arrête le listener."""
        pass

    @abstractmethod
    async def process_event(self, channel, event_type, data):
        """Traite un événement reçu."""
        pass

    @abstractmethod
    def send_event(self, channel, event_type, data):
        """Envoie un événement via le listener."""
        pass

class RedisListener(BaseEventListener):
    """Listener utilisant Redis pour la communication inter-processus."""
    
    def __init__(self):
        print("RedisListener init")
        try:
            from redis.asyncio import Redis
        except ImportError:
            raise ImportError(
                "You must install the redis package to use RedisListener for multiprocess event handling. \n pip install redis"
            )
        self.redis_client = Redis(**settings.EVENTSTREAM_REDIS)
        self.pubsub = self.redis_client.pubsub()
        self.running = False
        logger.info("RedisListener initialisé")

    def send_event(self, channel, event_type, data):
        """Envoie un événement via Redis."""
        import json
        import asyncio
        event_message = {
            "channel": channel,
            "event_type": event_type,
            "data": data,
        }
        logger.info(f"RedisListener.send_event: Envoi de l'événement {event_type} sur le canal {channel}")
        try:
            # Créer une nouvelle boucle d'événements si nécessaire
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(self.redis_client.publish("events_channel", json.dumps(event_message)))
            logger.info("RedisListener.send_event: Événement publié avec succès")
        except Exception as e:
            logger.error(f"RedisListener.send_event: Erreur lors de la publication de l'événement: {e}")

    async def process_event(self, channel, event_type, data):
        e = Event(channel, event_type, data)
        from .views import get_listener_manager
        get_listener_manager().add_to_queues(channel, e)

    async def listen(self):
        await self.pubsub.subscribe("events_channel")
        async for message in self.pubsub.listen():
            if message["type"] == "message":
                event_data = json.loads(message["data"])
                await self.process_event(
                    event_data["channel"],
                    event_data["event_type"],
                    event_data["data"]
                )

    async def start(self):
        self.running = True
        await self.listen()

    def stop(self):
        self.running = False

class FileSystemListener(BaseEventListener):
    """Listener utilisant le système de fichiers pour la communication inter-processus."""
    
    def __init__(self):
        print("FileSystemListener init")
        from .eventstream import file_ipc
        self.file_ipc = file_ipc
        self.running = False
        logger.info("FileSystemListener initialisé")

    def send_event(self, channel, event_type, data):
        """Envoie un événement via le système de fichiers."""
        import json
        event_message = {
            "channel": channel,
            "event_type": event_type,
            "data": data,
        }
        logger.info(f"FileSystemListener.send_event: Envoi de l'événement {event_type} sur le canal {channel}")
        try:
            self.file_ipc.write_event(event_message)
            logger.info("FileSystemListener.send_event: Événement écrit avec succès")
        except Exception as e:
            logger.error(f"FileSystemListener.send_event: Erreur lors de l'écriture de l'événement: {e}")

    async def process_event(self, channel, event_type, data):
        """Traite un événement reçu."""
        logger.info(f"FileSystemListener.process_event: Traitement de l'événement {event_type} sur le canal {channel}")
        try:
            e = Event(channel, event_type, data)
            from .views import get_listener_manager
            get_listener_manager().add_to_queues(channel, e)
            logger.info("FileSystemListener.process_event: Événement ajouté aux files d'attente")
        except Exception as e:
            logger.error(f"FileSystemListener.process_event: Erreur lors du traitement de l'événement: {e}")

    async def poll_events(self):
        """Sonde les événements du système de fichiers."""
        logger.info("FileSystemListener.poll_events: Démarrage du polling")
        while self.running:
            try:
                events = self.file_ipc.read_events()
                if events:
                    logger.info(f"FileSystemListener.poll_events: {len(events)} événements trouvés")
                    for event_data in events:
                        logger.info(f"FileSystemListener.poll_events: Traitement de l'événement {event_data['event_type']} sur le canal {event_data['channel']}")
                        await self.process_event(
                            event_data["channel"],
                            event_data["event_type"],
                            event_data["data"]
                        )
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"FileSystemListener.poll_events: Erreur lors du polling des événements: {e}")
                await asyncio.sleep(1)  # Attendre plus longtemps en cas d'erreur

    async def start(self):
        """Démarre le listener."""
        logger.info("FileSystemListener.start: Démarrage du listener")
        self.running = True
        await self.poll_events()

    def stop(self):
        """Arrête le listener."""
        logger.info("FileSystemListener.stop: Arrêt du listener")
        self.running = False

class InProcessListener(BaseEventListener):
    """Listener pour la communication dans le même processus."""
    
    def __init__(self):
        print("InProcessListener init")
        self.running = False
        logger.info("InProcessListener initialisé")

    def send_event(self, channel, event_type, data):
        """Envoie un événement directement dans le processus."""
        from .event import Event
        from .views import get_listener_manager
        e = Event(channel, event_type, data)
        get_listener_manager().add_to_queues(channel, e)

    async def process_event(self, channel, event_type, data):
        e = Event(channel, event_type, data)
        from .views import get_listener_manager
        get_listener_manager().add_to_queues(channel, e)

    async def start(self):
        self.running = True
        # Pas besoin de polling pour le listener in-process

    def stop(self):
        self.running = False

class GripListener(BaseEventListener):
    """Listener utilisant GRIP pour la communication."""
    
    def __init__(self):
        print("GripListener init")
        self.running = False
        logger.info("GripListener initialisé")

    def send_event(self, channel, event_type, data):
        """Envoie un événement via GRIP."""
        from .utils import publish_event
        publish_event(channel, event_type, data, None, None)

    async def process_event(self, channel, event_type, data):
        from .utils import publish_event
        publish_event(channel, event_type, data, None, None)

    async def start(self):
        self.running = True
        # Pas besoin de polling pour le listener Grip

    def stop(self):
        self.running = False 