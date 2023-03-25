import random

import pytest
from ultima import ultimap
from epic.common.general import get_single

from epic.bitstore import GSRaw, Composite, NotFoundInStore, InvalidURI, StoreNotAvailable

from .helpers import DictStore

# source: https://cloud.google.com/storage/docs/public-datasets/landsat
GS_PUBLIC_LANDSAT_URI = "gs://gcp-public-data-landsat/LC08/01/001/003/LC08_L1GT_001003_20140812_20170420_01_T2/" \
                        "LC08_L1GT_001003_20140812_20170420_01_T2_MTL.txt"


class TestGcp:
    def test_public_data(self):
        gs = GSRaw.anonymous()
        landsat = gs.get(GS_PUBLIC_LANDSAT_URI)
        self._verify_landsat(landsat)
        composite = Composite()
        cache = DictStore(writeable=True, prefix=None)
        composite.append_cache(cache)
        composite.append_source(gs, cache_result=True)
        assert composite.get(GS_PUBLIC_LANDSAT_URI) == landsat
        assert cache.get(GS_PUBLIC_LANDSAT_URI) == landsat

    @staticmethod
    def _verify_landsat(landsat):
        assert landsat.startswith(b'GROUP = L1_METADATA_FILE')

    def test_errors(self):
        gs = GSRaw.anonymous()
        with pytest.raises(TypeError):
            gs.get(None)
        invalid_paths = [
            "xx://whatever",
            "this is not a string",
            "bugs://not_today_bugs",
            "gs://",
            "gs://just_bucket",
            "gs://just_bucket/",
        ]
        for path in invalid_paths:
            with pytest.raises(InvalidURI):
                gs.get(path)
        with pytest.raises(NotFoundInStore):
            gs.get(f"{GS_PUBLIC_LANDSAT_URI}_{random.random()}")
        with pytest.raises(NotFoundInStore):
            gs.get(f"gs://this_bucket_does_not_exist_{random.random()}/some_key")

    def test_credentials_failure(self):
        gs = GSRaw({'these': 'are not valid'})
        with pytest.raises(StoreNotAvailable):
            gs.get(GS_PUBLIC_LANDSAT_URI)
        with pytest.raises(StoreNotAvailable):
            gs.put(GS_PUBLIC_LANDSAT_URI, b'data')

    def test_put(self):
        gs = GSRaw.anonymous()
        with pytest.raises(InvalidURI):
            gs.put("whatever", b'data')
        with pytest.raises(Exception, match="Anonymous credentials cannot be refreshed"):
            gs.put(GS_PUBLIC_LANDSAT_URI, b'data')

    @pytest.mark.parametrize('pre_client', [True, False])
    @pytest.mark.parametrize(
        ['backend', 'n_workers', 'n'], [
            ['multiprocessing', 4, 20],
            ['threading', 4, 20],
        ]
    )
    def test_parallel(self, backend, n_workers, n, pre_client):
        gs = GSRaw.anonymous()
        if pre_client:
            # there are potential issues when an existing client gets sent to workers, so we this case as well
            self._verify_landsat(gs.get(GS_PUBLIC_LANDSAT_URI))
        results = list(ultimap(gs.get, [GS_PUBLIC_LANDSAT_URI] * n, backend=backend, n_workers=n_workers))
        assert len(results) == n
        assert len(s := set(results)) == 1
        self._verify_landsat(get_single(s))
