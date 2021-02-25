import pytest
import gbif_dl


@pytest.fixture()
def doi():
    return "10.15468/dl.vnm42s"


@pytest.fixture(params=[None, -1, 2])
def nb_samples(request):
    return request.param


def test_is_doi(doi):
    assert gbif_dl.dwca.is_doi(doi) is True
    assert gbif_dl.dwca.is_doi("xyz") is False


def test_doi_to_gbif_key(doi):
    assert gbif_dl.dwca.doi_to_gbif_key(doi) == "0117522-200613084148143"


def test_url_generator(doi):
    """Test if to query a single url"""
    data_generator = gbif_dl.dwca.generate_urls(
        doi, dwca_root_path="test_dwca", label=None, delete=True
    )
    item = next(data_generator)
    assert item["url"]
