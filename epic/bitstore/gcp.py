import os
import re

from epic.logging import class_logger

from .store import Store
from .exc import StoreNotAvailable, NotFoundInStore

ANONYMOUS = 'anonymous'


class GSRaw(Store):
    URI_REGEX = "^gs://([^/]+)/(.+)$"
    URI_HINT = "gs://bucket/..."

    logger = class_logger

    def __init__(self, credentials=None):
        self.credentials = credentials
        self._cached_gs_client = None
        self._cached_gs_client_initialization_pid = None

    @classmethod
    def anonymous(cls):
        return cls(ANONYMOUS)

    def __getstate__(self):
        d = self.__dict__.copy()
        d['_cached_gs_client'] = None
        return d

    def is_valid(self, uri):
        return re.match(self.URI_REGEX, uri) is not None

    def get(self, uri):
        from google.cloud import exceptions
        self._check_valid(uri)
        bucket_name, path = re.match(self.URI_REGEX, uri).groups()
        if self._gs_client is None:
            raise StoreNotAvailable(self)
        bucket = self._gs_client.bucket(bucket_name)
        blob = bucket.blob(path)
        try:
            return blob.download_as_bytes()
        except exceptions.NotFound as exc:
            self.logger.debug(f"uri {uri} not found", exc_info=True)
            raise NotFoundInStore(self, uri) from exc

    @property
    def _gs_client(self):
        # note: this condition also covers the case of plain not having been initialized
        if self._cached_gs_client_initialization_pid != os.getpid():
            if self._cached_gs_client_initialization_pid is not None:
                self.logger.debug(
                    f"recreating _gs_client, was created in {self._cached_gs_client_initialization_pid} "
                    f"and we are in {os.getpid()}"
                )
            from google.cloud import storage
            try:
                if self.credentials == ANONYMOUS:
                    client = storage.Client.create_anonymous_client()
                else:
                    client_kwargs = {'credentials': self.credentials}
                    if self.credentials is not None:
                        client_kwargs['project'] = self.credentials.project_id
                    client = storage.Client(**client_kwargs)
            except Exception:
                self.logger.debug(f"failed to create client, this data source will not be available", exc_info=True)
                client = None
            else:
                self._configure_connection_pool_size(client)
            self._cached_gs_client = client
            self._cached_gs_client_initialization_pid = os.getpid()
        return self._cached_gs_client

    @staticmethod
    def _configure_connection_pool_size(client):
        from requests.adapters import HTTPAdapter
        # the default connection pool size is 10, not enough for high-scale work
        # noinspection PyProtectedMember
        client._http.mount("https://", HTTPAdapter(pool_maxsize=100))
        # noinspection PyProtectedMember
        client._http_internal.mount("https://", HTTPAdapter(pool_maxsize=100))

    def put(self, uri, data):
        self._check_valid(uri)
        if self._gs_client is None:
            raise StoreNotAvailable(self)
        bucket_name, key_name = re.match(self.URI_REGEX, uri).groups()
        self._gs_client.bucket(bucket_name).blob(key_name).upload_from_string(data)
