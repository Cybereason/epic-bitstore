import pickle

import pytest

from epic.bitstore import NotFoundInStore, InvalidURI, Composite

from .helpers import DictStore, RandomAPI


class TestDictStore:
    def test_read(self):
        ds = DictStore({"one": b'1'})
        assert not ds.is_valid("zzz")
        assert ds.is_valid("key:zzz")
        assert ds.get("key:one") == b'1'
        with pytest.raises(InvalidURI):
            ds.get("one")
        with pytest.raises(NotFoundInStore):
            ds.get("key:seven")
        with pytest.raises(Exception, match='does not implement'):
            ds.put("two", b'2')
        with pytest.raises(Exception, match='does not implement'):
            ds.put("key:two", b'2')

    def test_write(self):
        ds = DictStore(writeable=True)
        with pytest.raises(InvalidURI):
            ds.put("one", b'1')
        ds.put("key:one", b'1')
        assert ds.get("key:one") == b'1'
        ds.put("key:one", b'111')
        assert ds.get("key:one") == b'111'

    def test_composite(self):
        composite = Composite()
        assert not composite.is_valid("zzz")
        assert not composite.is_valid("key:zzz")
        with pytest.raises(InvalidURI):
            composite.get("zzz")
        with pytest.raises(InvalidURI):
            composite.get("key:zzz")

        ds1 = DictStore({"one": b'1', "two": b'2'})
        composite.append_source(ds1)
        assert not composite.is_valid("zzz")
        assert composite.is_valid("key:zzz")
        assert composite.get("key:one") == b'1'
        with pytest.raises(InvalidURI):
            composite.get("one")
        with pytest.raises(NotFoundInStore):
            composite.get("key:seven")
        with pytest.raises(Exception, match='does not implement'):
            composite.put("two", b'2')
        with pytest.raises(Exception, match='does not implement'):
            composite.put("key:two", b'2')

        ds2 = DictStore({"two": b'222', "three": b'333'})
        composite.append_source(ds2)
        assert not composite.is_valid("zzz")
        assert composite.is_valid("key:zzz")
        assert composite.get("key:one") == b'1'
        assert composite.get("key:two") == b'2'
        assert composite.get("key:three") == b'333'
        with pytest.raises(InvalidURI):
            composite.get("one")
        with pytest.raises(NotFoundInStore):
            composite.get("key:seven")

        ds3 = DictStore({"three": b'_3', "four": b'_4'}, prefix="clé")
        composite.append_source(ds3)
        assert not composite.is_valid("zzz")
        assert composite.is_valid("key:zzz")
        assert composite.is_valid("clé:zzz")
        assert composite.get("key:one") == b'1'
        assert composite.get("key:two") == b'2'
        assert composite.get("key:three") == b'333'
        assert composite.get("clé:three") == b'_3'
        assert composite.get("clé:four") == b'_4'
        with pytest.raises(InvalidURI):
            composite.get("one")
        with pytest.raises(NotFoundInStore):
            composite.get("key:seven")
        with pytest.raises(NotFoundInStore):
            composite.get("clé:seven")

        ds4 = DictStore({'precached': b'_pre'}, writeable=True)
        composite.append_cache(ds4)
        assert not composite.is_valid("zzz")
        assert composite.is_valid("key:zzz")
        assert composite.is_valid("clé:zzz")
        assert composite.get("key:one") == b'1'
        assert composite.get("key:two") == b'2'
        assert composite.get("key:three") == b'333'
        assert composite.get("clé:three") == b'_3'
        assert composite.get("clé:four") == b'_4'
        assert composite.get("key:precached") == b'_pre'
        with pytest.raises(InvalidURI):
            composite.get("one")
        with pytest.raises(NotFoundInStore):
            composite.get("key:seven")
        with pytest.raises(NotFoundInStore):
            composite.get("clé:seven")

        api1 = RandomAPI({'foo': 3, 'bar': 4})
        composite.append_source(api1, cache_result=True)
        assert not composite.is_valid("zzz")
        assert composite.is_valid("key:zzz")
        assert composite.is_valid("clé:zzz")
        assert composite.get("key:one") == b'1'
        assert composite.get("key:two") == b'2'
        assert composite.get("key:three") == b'333'
        assert composite.get("clé:three") == b'_3'
        assert composite.get("clé:four") == b'_4'
        assert composite.get("key:precached") == b'_pre'
        with pytest.raises(InvalidURI):
            composite.get("one")
        with pytest.raises(NotFoundInStore):
            composite.get("key:seven")
        with pytest.raises(NotFoundInStore):
            composite.get("clé:seven")
        with pytest.raises(NotFoundInStore):
            composite.get("clé:foo")
        rand_foo = composite.get("key:foo")
        assert rand_foo.startswith(b'3.')
        assert composite.get("key:foo") == rand_foo
        assert rand_foo != api1.get("key:foo")

        api2 = RandomAPI({'bar': 5, 'baz': 6})
        composite.append_source(api2, cache_result=False)
        rand_foo = composite.get("key:foo")
        assert rand_foo.startswith(b'3.')
        rand_bar_1 = composite.get("key:bar")
        assert rand_bar_1.startswith(b'4.')
        rand_bar_2 = composite.get("key:bar")
        assert rand_bar_1 == rand_bar_2
        rand_baz_1 = composite.get("key:baz")
        assert rand_baz_1.startswith(b'6.')
        rand_baz_2 = composite.get("key:baz")
        assert rand_baz_2.startswith(b'6.')
        assert rand_baz_1 != rand_baz_2

        assert list(ds4.contents.keys()) == ['key:precached', 'key:foo', 'key:bar']

        with pytest.raises(NotFoundInStore):
            try:
                composite.get("key:notfound")
            except NotFoundInStore as exc:
                raise pickle.loads(pickle.dumps(exc))

    def test_change_cache(self):
        ds = DictStore({'a': b'A', 'b': b'B'}, writeable=True)
        cache1 = DictStore(writeable=True)
        cache2 = DictStore(writeable=True)
        composite = Composite()
        composite.append_source(ds, cache_result=True)
        composite.append_cache(cache1)
        assert composite.get('key:a') == b'A'
        assert cache1.contents['key:a'] == b'A'
        assert 'key:a' not in cache2.contents
        composite.append_cache(cache2)
        assert composite.get('key:b') == b'B'
        assert cache2.contents['key:b'] == b'B'
        assert 'key:b' not in cache1.contents
        assert 'key:a' not in cache2.contents
        ds.put('key:a', b'AAA')
        assert composite.get('key:a') == b'AAA'
        assert cache1.contents['key:a'] == b'A'
        assert cache2.contents['key:a'] == b'AAA'
