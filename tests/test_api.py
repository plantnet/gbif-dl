import pytest
import gbif_dl


@pytest.fixture()
def queries():
    return {
        "speciesKey": [
            3189866,  # "Acer negundo L"
            3190653,  # "Ailanthus altissima (Mill.) Swingle"
        ],
        "datasetKey": ["50c9509d-22c7-4a22-a47d-8c48425ef4a7"],
    }


@pytest.fixture(params=[None, -1, 2])
def nb_samples(request):
    return request.param


def test_url_generator(queries):
    """Test if to query a single url"""
    data_generator = gbif_dl.api.generate_urls(
        queries=queries,
    )
    item = next(data_generator)
    assert item["url"]


def test_nb_samples(queries, nb_samples):
    data_generator = gbif_dl.api.generate_urls(queries=queries, nb_samples=nb_samples)
    if nb_samples is None:
        assert next(data_generator)
    elif nb_samples > 0:
        assert len(list(data_generator)) == nb_samples


def test_nb_samples_substreams(queries, nb_samples):
    data_generator = gbif_dl.api.generate_urls(
        queries=queries,
        nb_samples=nb_samples,
        split_streams_by="speciesKey",
    )
    if nb_samples is None:
        assert next(data_generator)
    elif nb_samples > 0:
        assert len(list(data_generator)) == nb_samples
