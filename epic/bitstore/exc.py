__all__ = ['StoreNotAvailable', 'NotFoundInStore', 'InvalidURI']


class ParameterizedException(Exception):
    def __init__(self, msg, *params):
        self._params = params
        super().__init__(msg)

    def __reduce__(self):
        return type(self), self._params


class StoreNotAvailable(ParameterizedException):
    def __init__(self, store):
        super().__init__(f"{store} is not available", str(store))


class NotFoundInStore(ParameterizedException):
    def __init__(self, store, uri):
        super().__init__(f"{store} does not contain '{uri}'", str(store), str(uri))


class InvalidURI(ParameterizedException):
    def __init__(self, store, uri, hint=None):
        store_class_name = store if isinstance(store, str) else store.__class__.__name__
        uri = str(uri)
        hint_str = '' if hint is None else f' (hint: {hint})'
        self._params = store_class_name, uri, hint_str
        super().__init__(f"URI '{uri}' is invalid for {store_class_name}{hint_str}", store_class_name, uri, hint)
