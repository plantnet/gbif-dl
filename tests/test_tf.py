import pytest
from gbif_dl.dataloaders import tensorflow as tfloader


@pytest.fixture
def train_urls(request):
    return [
        {
            "url": "https://bs.plantnet.org/image/o/6d5ed1f1769b4818ed5a234670dba742bf5b28a5",
            "basename": "e75239cd029162c81f16a6d6afb1057d2437bcc8",
            "label": "3189866",
        }
    ]


@pytest.fixture
def test_urls(request):
    return [
        {
            "url": "https://bs.plantnet.org/image/o/f32365ec997bdf06b57adcfca6a49c6d9602b321",
            "basename": "e04a36f124b875a16b5393a8fdef36846ada8e35",
            "label": "3189866",
        }
    ]


@pytest.mark.skip(reason="no way of currently testing this")
def test_torchdataset(train_urls, test_urls):
    """Currently failes because some async stuff"""
    train_dataset = tfloader.create_dataset_from_generator(root="train", generator=train_urls)
    test_dataset = tfloader.create_dataset_from_generator(root="test", generator=test_urls)
    assert len(train_dataset) == 1
    assert len(test_dataset) == 1
