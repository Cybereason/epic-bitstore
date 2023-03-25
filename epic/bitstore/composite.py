from epic.logging import class_logger

from .store import Store
from .exc import NotFoundInStore, StoreNotAvailable


class Composite(Store):
    logger = class_logger

    def __init__(self):
        self.sources: list[Store] = []
        self.cache_back: set[int] = set()
        self.cache: Store | None = None

    def append_source(self, source: Store, cache_result=False):
        self.sources.append(source)
        if cache_result:
            self.cache_back.add(id(source))

    def append_cache(self, cache: Store, read=True):
        if read:
            self.sources.append(cache)
        if self.cache is not None:
            self.logger.warning(f"replacing configured cache {self.cache} with {cache}")
        self.cache = cache
    
    def is_valid(self, uri):
        return any(store.is_valid(uri) for store in self.sources)
    
    def get(self, uri):
        self._check_valid(uri)
        for store in self.sources:
            if not store.is_valid(uri):
                continue
            try:
                data = store.get(uri)
            except (NotFoundInStore, StoreNotAvailable):
                continue
            if id(store) in self.cache_back and self.cache is not None and store is not self.cache:
                self._write_to_cache(data, uri)
            return data
        raise NotFoundInStore(self, uri)
    
    def _write_to_cache(self, data, uri):
        self.cache.put(uri, data)
