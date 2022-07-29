import pytest
import gbif_dl
import asyncio


@pytest.fixture
def urls(request):
    return [
        {
            "url": "https://bs.plantnet.org/image/o/6d5ed1f1769b4818ed5a234670dba742bf5b28a5",
            "basename": "e75239cd029162c81f16a6d6afb1057d2437bcc8",
            "label": "3189866",
            "subset": "train",
        },
        {
            "url": "https://bs.plantnet.org/image/o/f32365ec997bdf06b57adcfca6a49c6d9602b321",
            "basename": "e04a36f124b875a16b5393a8fdef36846ada8e35",
            "label": "3189866",
            "subset": "test",
        },
    ]


@pytest.fixture
def bad_urls(request):
    return ["https://bs.plantnet.org/image/o/wronghash"]


async def async_gen_from_list(rows):
    for i in rows:
        yield i
        await asyncio.sleep(1)


def gen_from_list(rows):
    for i in rows:
        yield i


def test_download_list(urls):
    gbif_dl.stores.dl_async.download(urls, root="root")


def test_download_fromgen(urls):
    """Currently failes because some async stuff"""
    gbif_dl.stores.dl_async.download(gen_from_list(urls), root="root")


def test_download_fromasybc(urls):
    gbif_dl.stores.dl_async.download(async_gen_from_list(urls), root="root")


def test_download_error(bad_urls):
    stats = gbif_dl.stores.dl_async.download(bad_urls)
    assert stats["failed"] == 1
