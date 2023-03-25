# Epic bitstore &mdash; Multi-tiered cloud-backed blob storage system
[![Epic-bitstore CI](https://github.com/Cybereason/epic-bitstore/actions/workflows/ci.yml/badge.svg)](https://github.com/Cybereason/epic-bitstore/actions/workflows/ci.yml)

## What is it?

The **epic-bitstore** Python library provides client access to a multi-tiered blob storage system based on cloud
backends, with the option to use API backends and caching mechanisms as well.

## Usage

For example, let's assume you store blobs in the following locations:
1. In an AWS bucket: `s3://aws_customer_data/files/<sha1>`
2. In a GCP bucket: `gs://gcp_customer_data/blobs/<sha1>`
3. In another GCP bucket: `gs://my_project/more_files/<sha1>`

Using epic-bitstore, you could fetch a blob from any of these storages using a single `get` command. The library would
iterate on the sources in order and would retrieve the data from the first matching source. This could also run in
parallel on multiple blobs, backed by the [ultima](https://github.com/Cybereason/ultima) parallelization library.

To implement the above strategy, you create a `Composite` store, and add each of your sources in order:
```python
from epic.bitstore import Sha1Composite, Sha1Store, S3Raw, GSRaw

blob_store = Sha1Composite()
blob_store.append_source(Sha1Store(S3Raw(), "s3://aws_customer_data/files/"))
blob_store.append_source(Sha1Store(GSRaw(), "gs://gcp_customer_data/blobs/"))
blob_store.append_source(Sha1Store(GSRaw(), "gs://my_project/more_files/"))

data = blob_store.get("4bc39c7d87318382feb3cc5a684c767fbd913968")
```

You can then use parallelization to efficiently map an iterator of hashes into an iterator of byte buffers:
```python
from ultima import ultimap

data_iter = ultimap(blob_store.get, iter_hashes, backend='threading', n_workers=16)
```

## API sources and caching layers

Let's also assume that you have an API that can retrieve blobs given their SHA1.
You would like to use it for fetching blobs, but only when they're not found in the above "passive" sources.

You can implement a `Sha1APISource` for your API, and add it to the `Composite` object:
```python
from epic.bitstore import Sha1APISource

class MyAPIStore(Sha1APISource):
    def __init__(self, api_client):
        super().__init__()
        self.api_client = api_client
    
    def api_get(self, sha1):
        # return None for a blob that can't be found
        return self.api_client.get_bytes(sha1)

# ctd after adding the three passive stores
blob_store.append_source(MyAPIStore(my_api_client))
```

API sources are often expensive, either in cost or performance.
You can add a caching store, and configure the API source to store fetched blobs into the cache.
It is important to append the cache *before* adding the API source, so that its cached blobs have precedence.

Append the cache and the API source:
```python
from epic.bitstore import Sha1Cache, GSRaw

# ctd after adding the three passive stores
blob_store.append_cache(Sha1Cache(GSRaw(), "gs://cache_for_api/"))
blob_store.append_store(MyAPIStore(my_api_client))
```

Now, when you retrieve a missing blob for the first time, the API is used; and after that the cache is used.
