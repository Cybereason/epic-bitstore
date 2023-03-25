import json
import random

import pytest
from ultima import ultimap
from epic.common.general import get_single

from epic.bitstore import S3Raw, Composite, NotFoundInStore, InvalidURI, StoreNotAvailable

from .helpers import DictStore

# source: https://registry.opendata.aws/usgs-lidar/
S3_PUBLIC_CATALOG_URI = "s3://usgs-lidar-stac/ept/catalog.json"


class TestAws:
    def test_public_data(self):
        s3 = S3Raw.anonymous()
        catalog = s3.get(S3_PUBLIC_CATALOG_URI)
        self._verify_catalog(catalog)
        composite = Composite()
        cache = DictStore(writeable=True, prefix=None)
        composite.append_cache(cache)
        composite.append_source(s3, cache_result=True)
        assert composite.get(S3_PUBLIC_CATALOG_URI) == catalog
        assert cache.get(S3_PUBLIC_CATALOG_URI) == catalog

    @staticmethod
    def _verify_catalog(catalog):
        assert json.loads(catalog)["type"] == "Catalog"

    def test_errors(self):
        s3 = S3Raw.anonymous()
        with pytest.raises(TypeError):
            s3.get(None)
        invalid_paths = [
            "xx://whatever",
            "this is not a string",
            "bugs3://not_today_bugs",
            "s3://",
            "s3://just_bucket",
            "s3://just_bucket/",
        ]
        for path in invalid_paths:
            with pytest.raises(InvalidURI):
                s3.get(path)
        with pytest.raises(NotFoundInStore):
            s3.get(f"s3://this_bucket_does_not_exist/some_key/{random.random()}")

    def test_credentials_failure(self):
        s3 = S3Raw([])
        with pytest.raises(StoreNotAvailable):
            s3.get(S3_PUBLIC_CATALOG_URI)
        with pytest.raises(StoreNotAvailable):
            s3.put(S3_PUBLIC_CATALOG_URI, b'data')

    def test_put(self):
        s3 = S3Raw.anonymous()
        with pytest.raises(InvalidURI):
            s3.put("whatever", b'data')
        with pytest.raises(Exception, match="An error occurred .AccessDenied. when calling the PutObject operation"):
            s3.put(S3_PUBLIC_CATALOG_URI, b'data')

    @pytest.mark.parametrize('pre_client', [True, False])
    @pytest.mark.parametrize(
        ['backend', 'n_workers', 'n'], [
            ['multiprocessing', 4, 20],
            ['threading', 4, 20],
        ]
    )
    def test_parallel(self, backend, n_workers, n, pre_client):
        s3 = S3Raw.anonymous()
        if pre_client:
            # there are potential issues when an existing client gets sent to workers, so we this case as well
            self._verify_catalog(s3.get(S3_PUBLIC_CATALOG_URI))
        results = list(ultimap(s3.get, [S3_PUBLIC_CATALOG_URI] * n, backend=backend, n_workers=n_workers))
        assert len(results) == n
        assert len(s := set(results)) == 1
        self._verify_catalog(get_single(s))
