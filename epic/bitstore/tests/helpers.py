import random

from epic.bitstore import Store, NotFoundInStore


class DictStore(Store):
    def __init__(self, init_data={}, writeable=False, prefix='key'):
        self.writeable = writeable
        self.prefix = prefix
        self.contents = {(f"{prefix}:{key}" if self.prefix else key): value for key, value in init_data.items()}

    def is_valid(self, uri):
        return isinstance(uri, str) and (not self.prefix or uri.startswith(f"{self.prefix}:"))

    def get(self, uri):
        self._check_valid(uri)
        if uri not in self.contents:
            raise NotFoundInStore(self, uri)
        return self.contents[uri]

    def put(self, uri, data):
        if not self.writeable:
            return super().put(uri, data)
        self._check_valid(uri)
        self.contents[uri] = data


class RandomAPI(Store):
    def __init__(self, bases={}, prefix='key'):
        self.bases = bases
        self.prefix = prefix

    def is_valid(self, uri):
        return isinstance(uri, str) and uri.startswith(f"{self.prefix}:")

    def get(self, uri):
        self._check_valid(uri)
        key = uri.split(self.prefix, 1)[1][1:]
        if key not in self.bases:
            raise NotFoundInStore(self, uri)
        return str(self.bases[key] + random.random()).encode()
