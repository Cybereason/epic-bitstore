import re
import hashlib
from abc import ABC, abstractmethod

from epic.logging import class_logger

from .store import Store
from .exc import NotFoundInStore
from .composite import Composite

__all__ = [
    'Sha1FormatMixin', 'InvalidDataFound', 'VerifySha1Mixin', 'Sha1Store', 'Sha1APISource', 'Sha1Composite', 'Sha1Cache'
]


class Sha1FormatMixin:
    URI_HINT = "<sha1>"
    _SHA1_REGEX = re.compile("^[0-9a-fA-F]{40}$")

    def is_valid(self, uri):
        return re.match(self._SHA1_REGEX, uri) is not None


class InvalidDataFound(NotFoundInStore):
    """Data found in store appears to be invalid"""


class VerifySha1Mixin:
    logger = class_logger

    def _verify_data(self, data, sha1):
        calc_sha1 = hashlib.sha1(data).hexdigest()
        if sha1.lower() != calc_sha1:
            self.logger.warning(f"the provided sha1 {sha1} does not match the calculated sha1 {calc_sha1} of data")
            raise InvalidDataFound(self, sha1)
        return data


class Sha1Store(Sha1FormatMixin, VerifySha1Mixin, Store):
    def __init__(self, base_store, prefix, verify=False):
        self.base_store = base_store
        self.prefix = prefix
        self.verify = verify

    def get(self, sha1: str):
        self._check_valid(sha1)
        try:
            data = self.base_store.get(f"{self.prefix}{sha1.lower()}")
        except NotFoundInStore as exc:
            raise NotFoundInStore(self, sha1) from exc
        if self.verify:
            self._verify_data(data, sha1)
        return data

    def put(self, sha1, data):
        self._check_valid(sha1)
        if self.verify:
            self._verify_data(data, sha1)
        self.base_store.put(f"{self.prefix}{sha1.lower()}", data)


class Sha1APISource(Sha1FormatMixin, VerifySha1Mixin, Store, ABC):
    def __init__(self, verify=False):
        self.verify = verify

    def get(self, sha1):
        self._check_valid(sha1)
        data = self.api_get(sha1.lower())
        if data is None:
            raise NotFoundInStore(self, sha1)
        if self.verify:
            data = self._verify_data(data, sha1)
        return data

    @abstractmethod
    def api_get(self, sha1): pass


class Sha1Composite(Sha1FormatMixin, Composite):
    SHA1_URI_REGEX = re.compile("^(?:sha1://)?([0-9a-fA-F]{40})$")
    URI_HINT = "<sha1> or sha1://<sha1>"

    def is_valid(self, uri):
        return re.match(self.SHA1_URI_REGEX, uri) is not None

    def get(self, uri):
        self._check_valid(uri)
        [sha1] = re.match(self.SHA1_URI_REGEX, uri).groups()
        sha1 = sha1.lower()
        return super().get(sha1)


class Sha1Cache(Sha1Store):
    """
    This is a store which silently ignores put failures.
    """
    def put(self, sha1, data):
        try:
            super().put(sha1, data)
            self.logger.debug(f"saved result of {sha1} to binary cache")
        except InvalidDataFound:
            self.logger.debug(f"data for {sha1} is invalid and cannot be cached, silently ignoring")
        except Exception:
            self.logger.debug(f"failed caching blob for sha1 {sha1}, silently ignoring", exc_info=True)
