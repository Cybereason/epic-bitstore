import os
import re
from typing import Iterable
from contextlib import suppress

from epic.common.general import to_list
from epic.logging import class_logger

from .store import Store
from .exc import StoreNotAvailable, NotFoundInStore

ANONYMOUS = 'anonymous'


class S3Raw(Store):
    URI_REGEX = "^s3://([^/]+)/(.+)$"
    URI_HINT = "s3://bucket/..."

    logger = class_logger

    def __init__(self, credentials=None):
        self.credentials = to_list(credentials)
        self._cached_s3_client = None
        self._cached_s3_client_initialization_pid = None

    @classmethod
    def anonymous(cls):
        return cls(ANONYMOUS)

    def __getstate__(self):
        d = self.__dict__.copy()
        d['_cached_s3_client'] = None
        return d

    def is_valid(self, uri):
        return re.match(self.URI_REGEX, uri) is not None

    def get(self, uri):
        self._check_valid(uri)
        if self._s3_client is None:
            raise StoreNotAvailable(self)
        bucket_name, key_name = re.match(self.URI_REGEX, uri).groups()
        exceptions = self._s3_client.exceptions
        try:
            return self._s3_client.get_object(Bucket=bucket_name, Key=key_name)["Body"].read()
        except (exceptions.NoSuchKey, exceptions.InvalidObjectState, exceptions.NoSuchBucket) as exc:
            self.logger.debug(f"uri {uri} not found", exc_info=True)
            raise NotFoundInStore(self, uri) from exc

    @property
    def _s3_client(self):
        # note: this condition also covers the case of plain not having been initialized
        if self._cached_s3_client_initialization_pid != os.getpid():
            self._cached_s3_client = self._try_different_credentials(self.credentials)
            self._cached_s3_client_initialization_pid = os.getpid()
        return self._cached_s3_client

    def _try_different_credentials(self, creds: Iterable[dict[str, str] | None]):
        # we don't want to import in the module level, for when this dependency is not installed and this class not used
        import boto3

        for creds in creds:
            if creds == ANONYMOUS:
                return self._anonymous_client()
            kwargs = {} if creds is None else creds
            client = None
            with suppress(Exception):
                client = boto3.Session(**kwargs).client("s3")
            if client is not None and self._test_access(client):
                return client

    @staticmethod
    def _anonymous_client():
        from botocore import UNSIGNED
        from botocore.config import Config
        from botocore.session import Session
        return Session().create_client('s3', config=Config(signature_version=UNSIGNED))

    # note: override this method for better verification that credentials are sufficient
    def _test_access(self, client):
        with suppress(Exception):
            client.list_buckets()
            return True

    def put(self, uri, data):
        self._check_valid(uri)
        if self._s3_client is None:
            raise StoreNotAvailable(self)
        bucket_name, key_name = re.match(self.URI_REGEX, uri).groups()
        self._s3_client.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=data,
            ACL='private',
            StorageClass='STANDARD',
        )
