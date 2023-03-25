import pytest

from epic.bitstore import (
    Sha1Store, Sha1Composite, Sha1Cache, Sha1APISource, InvalidURI, NotFoundInStore, InvalidDataFound
)

from .helpers import DictStore


class TestSha1:
    def test_format_mixin(self):
        for writeable in [False, True]:
            store = Sha1Store(DictStore(writeable=writeable), prefix='key:')
            assert store.is_valid("a" * 40)
            assert store.is_valid("A" * 40)
            assert not store.is_valid("A" * 41)
            assert not store.is_valid("z" * 40)
            with pytest.raises(InvalidURI):
                store.get("a" * 41)
            with pytest.raises(NotFoundInStore):
                store.get("a" * 40)
            if writeable:
                with pytest.raises(InvalidURI):
                    store.put("a" * 41, b'data')
                store.put("a" * 40, b'data')
                assert store.get("a" * 40) == b'data'

    def test_sha1_composite(self):
        sha1_composite = Sha1Composite()
        assert sha1_composite.is_valid("a" * 40)
        assert sha1_composite.is_valid("A" * 40)
        assert not sha1_composite.is_valid("z" * 40)
        assert not sha1_composite.is_valid("a" * 41)
        store1 = Sha1Store(DictStore(writeable=True), prefix='key:')
        store2 = Sha1Store(DictStore(writeable=True), prefix='key:')
        sha1_composite.append_source(store1)
        sha1_composite.append_source(store2)
        store1.put("a" * 40, b'data1')
        store2.put("b" * 40, b'data2')
        assert sha1_composite.get("a" * 40) == b'data1'
        assert sha1_composite.get("b" * 40) == b'data2'
        assert sha1_composite.get("Aa" * 20) == b'data1'
        assert sha1_composite.get("B" * 40) == b'data2'
        # a sha1 composite converts keys to lowercase
        store2.base_store.put("key:" + "C" * 40, b'inaccessible')
        with pytest.raises(NotFoundInStore):
            sha1_composite.get("C" * 40)
        store2.base_store.put("key:" + "c" * 40, b'accessible')
        assert sha1_composite.get("c" * 40) == b'accessible'

    def test_verify_sha1(self):
        store = Sha1Store(DictStore(writeable=True), prefix='key:', verify=True)
        assert store.is_valid("a" * 40)
        assert store.is_valid("A" * 40)
        assert not store.is_valid("z" * 40)
        assert not store.is_valid("a" * 41)
        store.put("4bc39c7d87318382feb3cc5a684c767fbd913968", b'epic.bitstore')
        assert store.get("4bc39c7d87318382feb3cc5a684c767fbd913968") == b'epic.bitstore'
        store.put("4bc39c7d87318382feb3cc5a684c767fbd913968".upper(), b'epic.bitstore')
        assert store.get("4bc39c7d87318382feb3cc5a684c767fbd913968".upper()) == b'epic.bitstore'
        with pytest.raises(InvalidDataFound):
            store.put("a" * 40, b'incorrect data')
        # inject the invalid data into the underlying store
        store.base_store.put("key:" + "a" * 40, b'incorrect data')
        with pytest.raises(InvalidDataFound):
            store.get("a" * 40)

    def test_sha1_cache(self):
        sha1_composite = Sha1Composite()
        cache_backend = DictStore(writeable=True, prefix='cache')
        cache = Sha1Cache(cache_backend, "cache:")
        sha1_composite.append_cache(cache, read=True)
        store = Sha1Store(DictStore(writeable=True), prefix='key:')
        store.put('a' * 40, b'A')
        sha1_composite.append_source(store, cache_result=True)
        assert not cache_backend.contents
        assert sha1_composite.get('a' * 40) == b'A'
        assert cache_backend.contents == {'cache:' + 'a' * 40: b'A'}
        assert sha1_composite.get('a' * 40) == b'A'
        del store.base_store.contents['key:' + 'a' * 40]
        assert sha1_composite.get('a' * 40) == b'A'
        assert cache_backend.contents == {'cache:' + 'a' * 40: b'A'}
        assert sha1_composite.get('a' * 40) == b'A'
        assert not store.base_store.contents
        cache.verify = True
        store.put("b" * 40, b'B')
        store.put("4bc39c7d87318382feb3cc5a684c767fbd913968", b'epic.bitstore')
        assert 'cache:' + 'b' * 40 not in cache_backend.contents
        assert sha1_composite.get('b' * 40) == b'B'
        assert 'cache:' + 'b' * 40 not in cache_backend.contents
        assert sha1_composite.get("4bc39c7d87318382feb3cc5a684c767fbd913968") == b'epic.bitstore'
        assert 'cache:4bc39c7d87318382feb3cc5a684c767fbd913968' in cache_backend.contents
        assert sha1_composite.get("4bc39c7d87318382feb3cc5a684c767fbd913968") == b'epic.bitstore'
        cache.verify_sha1 = False
        cache_backend.writeable = False
        store.put("c" * 40, b'C')
        assert sha1_composite.get('c' * 40) == b'C'
        assert 'cache:' + 'c' * 40 not in cache_backend.contents
        assert sha1_composite.get('c' * 40) == b'C'

    def test_api_source(self):
        source = MockSourceAPI()
        assert source.counter == 0
        source.get("a" * 40)
        source.get("a" * 40)
        source.get("b" * 40)
        assert source.counter == 3

        source = MockSourceAPI()
        composite = Sha1Composite()
        composite.append_source(source)
        composite.get("a" * 40)
        composite.get("a" * 40)
        composite.get("A" * 40)
        assert source.counter == 3

        composite = Sha1Composite()
        composite.append_cache(cache := Sha1Cache(DictStore(writeable=True), "key:"))
        composite.append_source(source := MockSourceAPI(), cache_result=True)
        assert source.counter == 0
        composite.get("a" * 40)
        assert source.counter == 1
        assert composite.get("a" * 40) == composite.get("A" * 40)
        assert source.counter == 1
        assert composite.get("a" * 40) == cache.get("a" * 40)
        assert composite.get("A" * 40) == cache.get("a" * 40)
        assert composite.get("a" * 40) == composite.get("A" * 40)
        assert source.counter == 1
        assert composite.get("a" * 40) == source.get("A" * 40)
        assert source.get("a" * 40) == composite.get("A" * 40)
        assert source.counter == 3
        assert composite.get("a" * 40) != composite.get("b" * 40)
        assert source.counter == 4
        cache.put("c" * 40, b'some_data')
        assert composite.get("c" * 40) == cache.get("c" * 40)
        assert source.counter == 4


class MockSourceAPI(Sha1APISource):
    def __init__(self):
        self.counter = 0
        super().__init__(verify=False)

    def api_get(self, sha1):
        self.counter += 1
        # pretty hard to generate valid data given a sha1
        return f"data_for_{sha1}"
