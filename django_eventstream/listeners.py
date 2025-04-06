import asyncio
import json
import logging
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

import portalocker
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
            # Import du client Redis synchrone pour l'envoi
            import redis
        except ImportError:
            raise ImportError(
                "You must install the redis package to use RedisListener for multiprocess event handling. \n pip install redis"
            )

        # Stockage de la configuration Redis pour une utilisation ultérieure
        self.redis_settings = getattr(settings, "EVENTSTREAM_REDIS", {})

        # Client synchrone pour l'envoi des événements (plus fiable)
        self.sync_redis_client = redis.Redis(**self.redis_settings)
        self.running = False
        logger.info("RedisListener initialisé")

    def send_event(self, channel, event_type, data):
        """Envoie un événement via Redis de manière synchrone."""
        import json

        event_message = {
            "channel": channel,
            "event_type": event_type,
            "data": data,
        }
        logger.info(
            f"RedisListener.send_event: Envoi de l'événement {event_type} sur le canal {channel}"
        )
        try:
            # Utiliser le client synchrone préalablement créé
            self.sync_redis_client.publish("events_channel", json.dumps(event_message))
            logger.info(
                "RedisListener.send_event: Événement publié avec succès (synchrone)"
            )
        except Exception as e:
            logger.error(
                f"RedisListener.send_event: Erreur lors de la publication de l'événement: {e}"
            )

    async def process_event(self, channel, event_type, data):
        e = Event(channel, event_type, data)
        from .views import get_listener_manager

        get_listener_manager().add_to_queues(channel, e)

    async def listen(self):
        """Écoute les événements Redis avec un client créé dans la boucle courante."""
        try:
            # Importer le client Redis asynchrone ici pour être sûr qu'il utilise la boucle courante
            from redis.asyncio import Redis

            # Créer un nouveau client Redis asynchrone lié à la boucle courante
            redis_client = Redis(**self.redis_settings)
            pubsub = redis_client.pubsub()

            logger.info(
                "RedisListener.listen: Client Redis asynchrone créé dans la boucle courante"
            )

            try:
                await pubsub.subscribe("events_channel")
                logger.info(
                    "RedisListener.listen: Souscription au canal events_channel"
                )

                async for message in pubsub.listen():
                    if not self.running:
                        break

                    if message["type"] == "message":
                        event_data = json.loads(message["data"])
                        await self.process_event(
                            event_data["channel"],
                            event_data["event_type"],
                            event_data["data"],
                        )
            finally:
                # Nettoyage propre des ressources
                try:
                    await pubsub.unsubscribe("events_channel")
                    # await redis_client.close() #Deprecated
                    await redis_client.aclose()
                    logger.info(
                        "RedisListener.listen: Ressources Redis asynchrones libérées"
                    )
                except Exception as e:
                    logger.error(
                        f"RedisListener.listen: Erreur lors du nettoyage Redis async: {e}"
                    )

        except Exception as e:
            logger.error(f"RedisListener.listen: Erreur dans la boucle d'écoute: {e}")

    async def start(self):
        """Démarre le listener Redis."""
        self.running = True
        try:
            # Créer une nouvelle boucle d'événements si nécessaire
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Démarrer la tâche d'écoute dans la boucle d'événements
            self.listen_task = loop.create_task(self.listen())
            await self.listen_task
        except Exception as e:
            logger.error(f"RedisListener.start: Erreur lors du démarrage: {e}")
            self.running = False
            raise

    def stop(self):
        """Arrête le listener Redis."""
        self.running = False
        if hasattr(self, "listen_task"):
            self.listen_task.cancel()


class FileSystemListener(BaseEventListener):
    """Listener utilisant le système de fichiers pour la communication inter-processus."""

    def __init__(self):
        print("FileSystemListener init")
        super().__init__()
        self.running = False

        # Initialisation des fichiers IPC
        self.events_file = Path(tempfile.gettempdir()) / "django_eventstream_events_"
        logger.info(f"FileSystemListener.events_file: {self.events_file}")
        self.lock_file = Path(tempfile.gettempdir()) / "django_eventstream_lock"
        logger.info(f"FileSystemListener.lock_file: {self.lock_file}")

        # Créer les fichiers s'ils n'existent pas
        self.events_file.touch()
        self.lock_file.touch()

        logger.info("FileSystemListener initialisé")

    def write_event(self, event_data):
        """Écrit un événement dans le fichier avec verrouillage."""
        logger.info(
            f"FileSystemListener.write_event: Tentative d'écriture de l'événement: {event_data}"
        )
        with portalocker.Lock(self.lock_file, "r+") as _:
            with open(self.events_file, "a", encoding="utf-8") as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.write(json.dumps(event_data) + "\n")
                portalocker.unlock(f)
        logger.info("FileSystemListener.write_event: Événement écrit avec succès")

    def read_events(self):
        """Lit les événements du fichier avec verrouillage."""
        logger.info(
            "FileSystemListener.read_events: Tentative de lecture des événements"
        )
        events = []
        with portalocker.Lock(self.lock_file, "r+") as _:
            with open(self.events_file, "r+", encoding="utf-8") as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                events = f.readlines()
                f.truncate(0)
                portalocker.unlock(f)
        logger.info(f"FileSystemListener.read_events: {len(events)} événements lus")
        return [json.loads(e.strip()) for e in events]

    def send_event(self, channel, event_type, data):
        """Envoie un événement via le système de fichiers."""
        logger.info(
            f"FileSystemListener.send_event: Envoi de l'événement {event_type} sur le canal {channel}"
        )
        event_message = {
            "channel": channel,
            "event_type": event_type,
            "data": data,
        }
        try:
            self.write_event(event_message)
            logger.info("FileSystemListener.send_event: Événement envoyé avec succès")
        except Exception as e:
            logger.error(f"FileSystemListener.send_event: Erreur lors de l'envoi: {e}")
            raise

    async def process_event(self, channel, event_type, data):
        """Traite un événement reçu."""
        logger.info(
            f"FileSystemListener.process_event: Traitement de l'événement {event_type} sur le canal {channel}"
        )
        try:
            e = Event(channel, event_type, data)
            from .views import get_listener_manager

            get_listener_manager().add_to_queues(channel, e)
            logger.info(
                "FileSystemListener.process_event: Événement traité avec succès"
            )
        except Exception as e:
            logger.error(
                f"FileSystemListener.process_event: Erreur lors du traitement: {e}"
            )
            raise

    async def poll_events(self):
        """Poll les événements du fichier."""
        logger.info("FileSystemListener.poll_events: Démarrage du polling")
        while self.running:
            try:
                events = self.read_events()
                for event in events:
                    logger.info(
                        f"FileSystemListener.poll_events: Événement reçu: {event}"
                    )
                    await self.process_event(
                        event["channel"], event["event_type"], event["data"]
                    )
                await asyncio.sleep(0.1)  # Petit délai pour éviter de surcharger le CPU
            except Exception as e:
                logger.error(
                    f"FileSystemListener.poll_events: Erreur lors du polling: {e}"
                )
                await asyncio.sleep(1)  # Délai plus long en cas d'erreur

    async def start(self):
        """Démarre le listener."""
        logger.info("FileSystemListener.start: Démarrage du listener")
        self.running = True
        try:
            # Créer une nouvelle boucle d'événements si nécessaire
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Démarrer la tâche de polling dans la boucle d'événements
            self.poll_task = loop.create_task(self.poll_events())
            await self.poll_task
        except Exception as e:
            logger.error(f"FileSystemListener.start: Erreur lors du démarrage: {e}")
            self.running = False
            raise

    def stop(self):
        """Arrête le listener."""
        logger.info("FileSystemListener.stop: Arrêt du listener")
        self.running = False
        if hasattr(self, "poll_task"):
            self.poll_task.cancel()


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
        from .views import get_listener_manager

        e = Event(channel, event_type, data)
        get_listener_manager().add_to_queues(channel, e)

    async def start(self):
        self.running = True
        # Pas besoin de polling pour le listener in-process

    def stop(self):
        self.running = False


# class GripListener(BaseEventListener):
#     """Listener utilisant GRIP pour la communication."""

#     def __init__(self):
#         print("GripListener init")
#         self.running = False
#         logger.info("GripListener initialisé")

#     def send_event(self, channel, event_type, data):
#         """Envoie un événement via GRIP."""
#         from .utils import publish_event

#         publish_event(channel, event_type, data, None, None)

#     async def process_event(self, channel, event_type, data):
#         from .utils import publish_event

#         publish_event(channel, event_type, data, None, None)

#     async def start(self):
#         self.running = True
#         # Pas besoin de polling pour le listener Grip

#     def stop(self):
#         self.running = False
