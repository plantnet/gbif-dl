import pytest
import gbifds
import asyncio


@pytest.fixture
def urls(request):
    return [
        {
            'url': 'https://bs.plantnet.org/image/o/6d5ed1f1769b4818ed5a234670dba742bf5b28a5',
            'basename': 'e75239cd029162c81f16a6d6afb1057d2437bcc8',
            'label': '3189866',
            'content_type': 'image/jpeg',
            'suffix': '.jpg'
        },
        {
            'url': 'https://bs.plantnet.org/image/o/f32365ec997bdf06b57adcfca6a49c6d9602b321',
            'basename': 'e04a36f124b875a16b5393a8fdef36846ada8e35',
            'label': '3189866',
            'content_type': 'image/jpeg',
            'suffix': '.jpg'
        },
        {
            'url': 'https://bs.plantnet.org/image/o/6d3498686f936ebbd0d48922097b2786413791bf',
            'basename': 'a50e854d85c8dcb0f87dc9d34d3c9f79f59a8f34',
            'label': '3189866',
            'content_type': 'image/jpeg',
            'suffix': '.jpg'
        }
    ]



async def async_gen_from_list(rows):
    for i in rows:
        yield i
        await asyncio.sleep(1)


def gen_from_list(rows):
    for i in rows:
        yield i


def test_download_list(urls):
    gbifds.io.download(urls, root="root")


@pytest.mark.xfail
def test_download_fromgen(urls):
    """Currently failes because some async stuff"""
    gbifds.io.download(gen_from_list(urls), root="root")


def test_download_fromasybc(urls):
    gbifds.io.download(async_gen_from_list(urls), root="root")


