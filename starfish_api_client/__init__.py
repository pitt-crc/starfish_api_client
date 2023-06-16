import importlib.metadata

from .client import *

try:
    __version__ = importlib.metadata.version('quota-notifier')

except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = '0.0.0'
