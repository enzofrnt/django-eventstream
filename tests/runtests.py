#!/usr/bin/env python
import os
import sys
import django
from django.conf import settings

# Configuration de Django pour les tests
os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'
django.setup()

# Import des tests
from django.test.runner import DiscoverRunner
from django.test.utils import get_runner

def run_tests(*args):
    TestRunner = get_runner(settings)
    test_runner = TestRunner()
    failures = test_runner.run_tests([
        "tests.test_storage",
        "tests.test_stream",
        "tests.test_listeners",
        "tests.test_e2e"
    ])
    sys.exit(bool(failures))

if __name__ == '__main__':
    run_tests(*sys.argv[1:])
