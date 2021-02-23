import pytest
import gbif_dl
import asyncio


@pytest.fixture()
def queries():
    return {
        "speciesKey": [
            3189866,  # "Acer negundo L"
        ],
        "datasetKey": ["50c9509d-22c7-4a22-a47d-8c48425ef4a7"],
    }


def test_url_generator(queries):
    data_generator = gbif_dl.api.generate_urls(
        queries=queries,
    )
    assert len(list(data_generator)) > 0
