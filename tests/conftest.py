"""Shared helpers for the offline tests (no network, no BigQuery). Run: pytest tests/"""
import importlib.util, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "tests", "fixtures")


def load(rel_path, name):
    """Import a file by path under its own module name (cloud/feeds/main.py and cloud/market/main.py are both 'main')."""
    folder = os.path.join(ROOT, os.path.dirname(rel_path))
    if folder not in sys.path: sys.path.insert(0, folder)
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, rel_path))
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def fixture(name):
    return open(os.path.join(FIXTURES, name), encoding="utf-8").read()
