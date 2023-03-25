from abc import ABC, abstractmethod

from .exc import InvalidURI


class Store(ABC):
    URI_HINT = None

    @abstractmethod
    def is_valid(self, uri): pass

    @abstractmethod
    def get(self, uri): pass

    def _check_valid(self, uri):
        if not self.is_valid(uri):
            raise InvalidURI(self, uri, hint=self.URI_HINT)

    # optional
    def put(self, uri, data):
        raise NotImplementedError(f"{self.__class__.__name__} does not implement the 'put' method")
