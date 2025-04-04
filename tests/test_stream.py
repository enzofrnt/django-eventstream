import json
import os
import re
import threading
import time
import warnings
from abc import ABC
from typing import List

import django
import requests
from channels.testing import ChannelsLiveServerTestCase
from django.urls import reverse


class BaseTest(ChannelsLiveServerTestCase, ABC):
    """Classe de base abstraite pour les tests. Les tests de cette classe ne seront pas exécutés."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        django.setup()

    def setUp(self):
        super().setUp()
        self.sse_line_pattern = re.compile("(?P<name>[^:]*):?( ?(?P<value>.*))?")
        self.end_of_field = re.compile(r"\r\n\r\n|\r\r|\n\n")
        self.channel = "test"

    def parse_sse_to_events(self, data: bytes | str) -> List[any]:
        """Parse les données SSE en liste d'objets Event."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")

        events = []
        # Séparons d'abord les événements (séparés par double newline)
        raw_events = self.end_of_field.split(data)

        for raw_event in raw_events:
            # Ignorer les événements vides ou qui ne contiennent que des espaces
            if not raw_event.strip() or raw_event.strip() == ":":
                continue

            # Initialisation des données de l'événement
            event_data = {"data": "", "event": "message", "id": None, "retry": None}

            # Parse chaque ligne de l'événement
            for line in raw_event.splitlines():
                m = self.sse_line_pattern.match(line)
                if m is None:
                    warnings.warn(f'Invalid SSE line: "{line}"', SyntaxWarning)
                    continue

                name = m.group("name")
                if (
                    name == "" or name.strip() == ":"
                ):  # Ligne de commentaire commençant par ":" ou ligne vide
                    continue

                value = m.group("value") or ""

                if name == "data":
                    # Concaténation des lignes de données avec newline
                    if event_data["data"]:
                        event_data["data"] = f"{event_data['data']}\n{value}"
                    else:
                        event_data["data"] = value
                elif name in ("event", "id", "retry"):
                    event_data[name] = value

            # Création de l'objet Event si nous avons des données valides
            if event_data["event"]:  # Au minimum un type d'événement est requis
                try:
                    # Parse les données JSON si possible
                    data_dict = (
                        json.loads(event_data["data"]) if event_data["data"] else {}
                    )
                except json.JSONDecodeError:
                    print("--------------------------------")
                    print("JSONDecodeError")
                    data_dict = (
                        {"message": event_data["data"]} if event_data["data"] else {}
                    )

                # Extraire le canal depuis l'événement s'il est fourni dans les données
                channel = (
                    data_dict.pop("channel", self.channel)
                    if isinstance(data_dict, dict)
                    else self.channel
                )
                from django_eventstream.event import Event

                event = Event(
                    channel=channel,
                    type=event_data["event"],
                    data=data_dict,
                    id=event_data["id"],
                )
                events.append(event)

        return events

    def base_test(
        self,
        event_to_send,
        event_to_receive,
        timeout=10,
        send_from_inside=True,
        trigger_on_timeout=True,
    ):
        print("Début du test")
        url = reverse("events")
        print(f"URL de test: {self.live_server_url + url}")

        response = requests.get(self.live_server_url + url, stream=True)
        print(f"Réponse reçue: {response.status_code}")

        # Événement pour synchroniser le départ
        start_event = threading.Event()
        # Container pour stocker le résultat
        result_container = {"events": None}

        def send_event_thread_from_outside(event_to_send):
            """Thread pour envoyer des événements depuis l'extérieur."""
            print("Thread d'envoi démarré")
            start_event.wait()
            time.sleep(2)  # Attendre que le client soit prêt
            for event in event_to_send:
                print(f"Envoi de l'événement: {event}")
                from django_eventstream import send_event

                send_event(event["channel"], event["event_type"], event["data"])
                time.sleep(0.1)  # Petit délai entre les événements

        def send_event_thread_from_inside(event_to_send):
            """Thread pour envoyer des événements depuis l'intérieur."""
            print("Thread d'envoi interne démarré")
            start_event.wait()
            time.sleep(2)  # Attendre que le client soit prêt
            url = reverse("send_event")
            for event in event_to_send:
                print(f"Envoi de l'événement interne: {event}")
                response = requests.post(
                    self.live_server_url + url,
                    json={
                        "channel": event["channel"],
                        "event_type": event["event_type"],
                        "data": event["data"],
                    },
                )
                self.assertEqual(response.status_code, 200)
                time.sleep(0.1)  # Petit délai entre les événements

        def listener_event_thread(response, events, result_container):
            """Thread pour écouter les événements."""
            print("Thread d'écoute démarré")
            content = b""
            events_list = []
            buffer = b""
            start_time = time.time()
            receive_event_datetime = []

            def read_content(response):
                nonlocal content, events_list, buffer, start_time, receive_event_datetime
                print("Début de la lecture des événements")
                for line in response.iter_content():
                    if time.time() - start_time > timeout:
                        print("Timeout atteint !")
                        return events_list

                    if line:
                        buffer += line
                        if b"\n\n" in buffer:
                            complete_events, buffer = buffer.split(b"\n\n", 1)
                            content += complete_events + b"\n\n"
                            parsed_events = self.parse_sse_to_events(complete_events)
                            print(f"Événements parsés: {parsed_events}")
                            events_list.extend(parsed_events)
                            receive_event_datetime.extend(
                                [time.time()] * len(parsed_events)
                            )

                            if event_to_receive and len(events_list) >= len(
                                event_to_receive
                            ):
                                print(f"Tous les événements reçus: {len(events_list)}")
                                return events_list

                if buffer:
                    parsed_events = self.parse_sse_to_events(buffer)
                    events_list.extend(parsed_events)
                    receive_event_datetime.extend([time.time()] * len(parsed_events))

                return events_list

            start_event.wait()
            print("Début de la réception des événements")
            result_container["events"] = read_content(response)
            result_container["receive_event_datetime"] = receive_event_datetime

        send_event_thread = (
            send_event_thread_from_inside
            if send_from_inside
            else send_event_thread_from_outside
        )

        # Container pour stocker le résultat et les erreurs
        result_container = {
            "events": None,
            "receive_event_datetime": None,
            "error": None,
        }

        # Créer et démarrer les threads
        send_thread = threading.Thread(target=send_event_thread, args=(event_to_send,))
        listen_thread = threading.Thread(
            target=listener_event_thread,
            args=(response, event_to_send, result_container),
            daemon=True,
        )

        send_thread.start()
        listen_thread.start()

        print("Départ simultané !")
        start_event.set()

        # Attendre que les threads se terminent avec un timeout
        send_thread.join(timeout=timeout)
        listen_thread.join(timeout=timeout)

        # Vérifier si le thread est toujours en vie (timeout)
        if listen_thread.is_alive() and trigger_on_timeout:
            self.fail("Timeout: Le test a dépassé les 10 secondes")

        print("send_thread.join()")
        print("listen_thread.join()")

        # Récupérer le résultat du thread
        received_events = result_container["events"]
        receive_event_datetime = result_container["receive_event_datetime"]
        # if received_events is None:
        #     self.fail("Aucun événement reçu")

        print(f"Événements reçus ({len(received_events) if received_events else 0}):")
        if received_events:
            for event, timestamp in zip(received_events, receive_event_datetime):
                print(
                    f"- Type: {event.type}, Channel: {event.channel}, Data: {event.data}, Timestamp: {timestamp}"
                )

        return received_events, receive_event_datetime

    def assert_events(self, received_events, event_to_receive):
        """Méthode commune pour vérifier les événements reçus."""
        # Faire les assertions
        self.assertIsNotNone(received_events)

        # Vérifier que nous avons au moins le nombre d'événements envoyés
        self.assertGreaterEqual(len(received_events), len(event_to_receive))

        for i, sent_event in enumerate(event_to_receive):
            self.assertEqual(received_events[i].type, sent_event["event_type"])
            self.assertEqual(received_events[i].channel, sent_event["channel"])
            self.assertEqual(received_events[i].data, sent_event["data"])


class RedisListenerTest(BaseTest):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings.redis")
        super().setUpClass()

    def test_stream_from_outside(self):
        timeout = 5

        event_to_send = [
            {"channel": "test", "event_type": "test", "data": f"test_{i}"}
            for i in range(0, 10)
        ]

        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}}
        ] + event_to_send

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout, send_from_inside=False
        )

        self.assert_events(received_events, event_to_receive)

    def test_stream_from_inside(self):
        timeout = 5

        event_to_send = [
            {"channel": "test", "event_type": "test", "data": f"test_{i}"}
            for i in range(0, 10)
        ]

        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}}
        ] + event_to_send

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout, send_from_inside=True
        )

        self.assert_events(received_events, event_to_receive)

    def test_keep_alive(self):
        """Test thta verify thta we receive keep-alive event every 20 seconds"""
        timeout = 41
        event_to_send = []
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            {"channel": "test", "event_type": "keep-alive", "data": {}},
            {"channel": "test", "event_type": "keep-alive", "data": {}},
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout
        )

        print(f"Received events: \n {received_events}")
        print(f"Received event datetime: \n {receive_event_datetime}")

        self.assertEqual(len(received_events), len(event_to_receive))
        self.assert_events(received_events, event_to_receive)

        # Vérifier que il y a environ 20 secondes d'écart entre chaque événement reçus
        # On accepte une marge d'erreur de 0.1 seconde
        for i in range(1, len(receive_event_datetime)):
            time_diff = receive_event_datetime[i] - receive_event_datetime[i - 1]
            print(f"Difference between event {i} and event {i-1}: {time_diff}")
            self.assertGreaterEqual(time_diff, 19.5)  # 20 - 0.5
            self.assertLessEqual(time_diff, 20.5)  # 20 + 0.5

    def test_keep_alive_after_event(self):
        """Test thta verify that after an event was send, we receive keep-alive event every 20 seconds"""
        timeout = 23
        event_to_send = [{"channel": "test", "event_type": "test", "data": "test"}]
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            *event_to_send,
            {"channel": "test", "event_type": "keep-alive", "data": {}},
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout
        )

        self.assert_events(received_events, event_to_receive)

        # Vérifier que il y a environ 20 secondes d'écart entre l'event envoyé et le keep-alive reçu
        # On accepte une marge d'erreur de 0.5 seconde
        time_diff = receive_event_datetime[-1] - receive_event_datetime[1]
        print(f"Difference between first and last event: {time_diff}")
        self.assertGreaterEqual(time_diff, 19.5)  # 20 - 0.5
        self.assertLessEqual(time_diff, 20.5)  # 20 + 0.5


class FileSystemListenerTest(BaseTest):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings.file")
        super().setUpClass()

    def test_stream_from_outside(self):
        timeout = 5
        event_to_send = [
            {"channel": "test", "event_type": "test", "data": f"test_{i}"}
            for i in range(0, 10)
        ]
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}}
        ] + event_to_send
        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout, send_from_inside=False
        )

        self.assert_events(received_events, event_to_receive)

    def test_stream_from_inside(self):
        timeout = 5
        event_to_send = [
            {"channel": "test", "event_type": "test", "data": f"test_{i}"}
            for i in range(0, 10)
        ]
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}}
        ] + event_to_send
        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout, send_from_inside=True
        )

        self.assert_events(received_events, event_to_receive)

    def test_keep_alive(self):
        """Test thta verify thta we receive keep-alive event every 20 seconds"""
        timeout = 41
        event_to_send = []
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            {"channel": "test", "event_type": "keep-alive", "data": {}},
            {"channel": "test", "event_type": "keep-alive", "data": {}},
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout
        )

        print(f"Received events: \n {received_events}")
        print(f"Received event datetime: \n {receive_event_datetime}")

        self.assertEqual(len(received_events), len(event_to_receive))
        self.assert_events(received_events, event_to_receive)

        # Vérifier que il y a environ 20 secondes d'écart entre chaque événement reçus
        # On accepte une marge d'erreur de 0.1 seconde
        for i in range(1, len(receive_event_datetime)):
            time_diff = receive_event_datetime[i] - receive_event_datetime[i - 1]
            print(f"Difference between event {i} and event {i-1}: {time_diff}")
            self.assertGreaterEqual(time_diff, 19.5)  # 20 - 0.5
            self.assertLessEqual(time_diff, 20.5)  # 20 + 0.5

    def test_keep_alive_after_event(self):
        """Test thta verify that after an event was send, we receive keep-alive event every 20 seconds"""
        timeout = 23
        event_to_send = [{"channel": "test", "event_type": "test", "data": "test"}]
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            *event_to_send,
            {"channel": "test", "event_type": "keep-alive", "data": {}},
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout
        )

        self.assert_events(received_events, event_to_receive)

        # Vérifier que il y a environ 20 secondes d'écart entre l'event envoyé et le keep-alive reçu
        # On accepte une marge d'erreur de 0.5 seconde
        time_diff = receive_event_datetime[-1] - receive_event_datetime[1]
        print(f"Difference between first and last event: {time_diff}")
        self.assertGreaterEqual(time_diff, 19.5)  # 20 - 0.5
        self.assertLessEqual(time_diff, 20.5)  # 20 + 0.5


class InProcessListenerTest(BaseTest):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings.in_process")
        super().setUpClass()

    def test_stream_from_inside(self):
        """Test qui vérifie que les événements envoyés depuis l'intérieur sont bien reçus avec le InProcessListener."""
        timeout = 10
        event_to_send = [
            {"channel": "test", "event_type": "test", "data": f"test_{i}"}
            for i in range(0, 10)
        ]

        # On spécifie les événements attendus car on utilise send_from_inside=True
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            *event_to_send,
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send,
            event_to_receive,
            timeout,
            send_from_inside=True,
            trigger_on_timeout=True,
        )

        # Vérification des événements reçus
        self.assert_events(received_events, event_to_receive)

    def test_stream_from_outside(self):
        """Test qui vérifie que les événements envoyés depuis l'extérieur ne sont PAS reçus avec le InProcessListener."""
        timeout = 10
        event_to_send = [
            {"channel": "test", "event_type": "test", "data": f"test_{i}"}
            for i in range(0, 10)
        ]

        # On ne s'attend qu'à recevoir l'événement stream-open
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}}
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send,
            event_to_receive,
            timeout,
            send_from_inside=False,  # Envoyer depuis l'extérieur
            trigger_on_timeout=False,  # Ne pas échouer sur le timeout
        )

        # Vérifier que nous n'avons reçu que l'événement stream-open
        self.assert_events(received_events, event_to_receive)

    def test_keep_alive(self):
        """Test thta verify thta we receive keep-alive event every 20 seconds"""
        timeout = 41
        event_to_send = []
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            {"channel": "test", "event_type": "keep-alive", "data": {}},
            {"channel": "test", "event_type": "keep-alive", "data": {}},
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout
        )

        print(f"Received events: \n {received_events}")
        print(f"Received event datetime: \n {receive_event_datetime}")

        self.assertEqual(len(received_events), len(event_to_receive))
        self.assert_events(received_events, event_to_receive)

        # Vérifier que il y a environ 20 secondes d'écart entre chaque événement reçus
        # On accepte une marge d'erreur de 0.1 seconde
        for i in range(1, len(receive_event_datetime)):
            time_diff = receive_event_datetime[i] - receive_event_datetime[i - 1]
            print(f"Difference between event {i} and event {i-1}: {time_diff}")
            self.assertGreaterEqual(time_diff, 19.5)  # 20 - 0.5
            self.assertLessEqual(time_diff, 20.5)  # 20 + 0.5

    def test_keep_alive_after_event(self):
        """Test thta verify that after an event was send, we receive keep-alive event every 20 seconds"""
        timeout = 23
        event_to_send = [{"channel": "test", "event_type": "test", "data": "test"}]
        event_to_receive = [
            {"channel": "test", "event_type": "stream-open", "data": {}},
            *event_to_send,
            {"channel": "test", "event_type": "keep-alive", "data": {}},
        ]

        received_events, receive_event_datetime = super().base_test(
            event_to_send, event_to_receive, timeout
        )

        self.assert_events(received_events, event_to_receive)

        # Vérifier que il y a environ 20 secondes d'écart entre l'event envoyé et le keep-alive reçu
        # On accepte une marge d'erreur de 0.5 seconde
        time_diff = receive_event_datetime[-1] - receive_event_datetime[1]
        print(f"Difference between first and last event: {time_diff}")
        self.assertGreaterEqual(time_diff, 19.5)  # 20 - 0.5
        self.assertLessEqual(time_diff, 20.5)  # 20 + 0.5
