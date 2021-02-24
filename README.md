# gbif-dl 🌱 > 💾

[![Build Status](https://github.com/plantnet/gbif-dl/workflows/CI/badge.svg)](https://github.com/plantnet/gbif-dl/actions?query=workflow%3ACI+branch%3Amaster+event%3Apush)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/gbif-dl.svg)](https://pypi.python.org/pypi/gbif-dl)

this package makes it simpler to obtain media data from the GBIF database to be used for training __machine learning classification__ tasks. It wraps the [GBIF API](https://www.gbif.org/developer/summary) and supports directly querying the api to obtain and download a list of urls.
Existing saved queries can also be obtained using the download api of GBIF simply by providing GBIF DOI key.
The package provides an efficient downloader that uses python asyncio modules to speed up downloading of many small files as typically occur in downloads.

## Installation

Installation can be done via pip.

```
pip install gbif-dl
```
## Usage

The usage of `gbif-dl` helps users to create their own GBIF based media pipeline for training machine learning models. The package provides two core functionalities as followed:

1. `gbif-dl.generators`: Generators provide image urls from the GBIF database given queries or a pre-defined URL.
2. `gbif-dl.io`: Provides efficient media downloading to write the data to a storage device.

### 1. Retrieve media urls from GBIF

`gbif-dl` supports two ways to retrieve image urls. One is to use directly query the gbif api the `gbif_dl.api` module. This is suited for quickly retrieving smaller datasets that do not require extensive query parameters. Another way is to use already the [gbif download workflows](https://www.gbif.org/data-processing) which assemble a [Darwin Core Archives](https://github.com/gbif/ipt/wiki/DwCAHowToGuide) waiting on the gbif servers. These can be downloaded and parsed using the `gbif_dl.dwca` module as explained below.

#### `gbif_dl.generators.api`: getting occurance media URLS by querying GBIF

The query supports all fields that are supported by the [GBIF occurance API](https://www.gbif.org/developer/occurrence#search). In the following example, we query three plants using the `speciesKey` of GBIF from the list of [top 1200 invasive plant species](https://www.cabi.org/ISC). Also, we are limiting the results by only retrieving results from [Plantnet](https://plantnet.org) _and_ [iNaturalist](https://www.inaturalist.org/). using the `datasetKey`.

The query is passed as a simple dictionary:

```python
queries = {
    "speciesKey": [
        5352251, # "Robinia pseudoacacia L"
        3190653, # "Ailanthus altissima (Mill.) Swingle"
        3189866  # "Acer negundo L"
    ],
    "datasetKey": [
        "7a3679ef-5582-4aaa-81f0-8c2545cafc81",  # plantnet
        "50c9509d-22c7-4a22-a47d-8c48425ef4a7"  # inaturalist
    ]
}
```


Give this query, we can pass this to the `api.generate_urls` function which returns a python
generator:

```python
import gbif_dl
data_generator = gbif_dl.api.generate_urls(
    queries=queries,
    label="speciesKey",
)
```

Additionally we have to specify the output `label` from the occurances which doesn't
necessarily have to be part of the query attributes. The `label` is later used to store the data in hierachical structure: `label/image.jpg`.

Iterating over the generator now yields the media data returning a few thousand urls.

```python
for i in data_generator:
    print(i)
```

each return entry is a dictionary of media attributes, to be consumed by the downloader.

```python
{
    'url': 'https://bs.plantnet.org/image/o/cfa25c7fb5cdf12719d1345769d3936d0ca73974', 
    'basename': 'fdcc3440ab0e3abf824a5c68c864b018cccfcd3b', 
    'label': '5352251'
},
{
    'url': 'https://static.inaturalist.org/photos/58881180/original.jpeg?1577914533', 
    'basename': '7db818c0708ba859516353ff9b30ef942aca19de', 
    'label': '3189866'
},
{
    'url': 'https://static.inaturalist.org/photos/58866788/original.jpeg?1577898729', 
    'basename': '58ae3ef46e59e9a06d67de09c8b7ef3b8db3c85a', 
    'label': '3189866'
}
```

#### Balancing items

Very often users won't be using all media downloads from a given query since this often results in datasets with heavily inbalanced number of samples per label. When generating urls from the API, users can specify certain additional attributes to influence the sampling process. For example, to balance the dataset by the dataset provider and by the species the following arguments can be used:

* `split_streams_by`: splits the query into combination of several substreams where each stream represents the product of the query values. When combined with `nb_samples`, this produces a balanced dataset where each stream yields the same number of samples.
* `nb_samples`: an integer that limits the total number of samples to be generated from the balanced streams. E.g, this can be used to just get `100` samples from the api. When set to `-1`, the minimum number of samples from all streams is used, hence this results in the __maximum number of balanced__ sampled from all streams.

In the following example, we will receive a balanced dataset assembled from `3 species * 2 datasets = 6 streams` and only get minumum number of total samples from all 6 streams:

```python
data_generator = gbif_dl.api.generate_urls(
    queries=queries,
    label="speciesKey",
    nb_samples=-1,
    split_streams_by=["datasetKey", "speciesKey"],
)
```

For other, more advanced, use-cases users can add more constraints:

* `nb_samples_per_stream`: put a hard __limit__ on the _maximum number of samples_ to be yielded by a stream.
* `weighted_streams`: weights each stream by its original distribution. That way users can get a smaller subset of the data but keep the original __unbalanced__ distribution of the data.

The following dataset consist of exactly 1000 samples for which the distribution of `speciesKey` is maintained from the full query of all samples. Furthermore, we only allow a maxmimum of 800 samples per species.

```python
data_generator = gbifmediads.api.generate_urls(
    queries=queries,
    label="speciesKey",
    nb_samples=1000,
    nb_samples_per_stream=800,
    weighted_streams=True,
    split_streams_by=["speciesKey"],
)
```

### Get URLS using Darwin Core Archives

A url generator can also be created from a GBIF download link given a registered DOI or a GBIF download ID. In the following example we will be downloading and parse DWCA archive [that should yield the same results as in the query example above.](https://www.gbif.org/occurrence/download/0117522-200613084148143).

* `dwca_root_path`: Set root path where to store the DWCA zip files. Defaults to None, which results in the creation of a temporary directory, If the path and DWCA archive already exist, it will not be downloaded again.

The following example creates a data_generator with the the same output class label as in the example above.

```python
data_generator = gbif_dl.dwca.generate_urls(
    "10.15468/dl.vnm42s", dwca_root_path="dwcas", label="speciesKey"
)
```
### Downloading images to disk

Downloading from a url generator can simply be done by running.

```python
gbif_dl.io.download(data_generator, root="my_dataset")
```

The downloader provides very fast download speeds by using an async queue. Some fail-safe functionality is provided by setting the number of `retries`, default to 3.

### Training Datasets

#### PyTorch

`gbif-dl` makes it simple to train a PyTorch image classification model by using e.g. `torchvision.ImageFolder`. Each item in the `data_generator` can be randomly assigned to a `train` or `test` subset using `random_subsets`. That way users can directly use the subsets.

```python
import torchvision
gbif_dl.io.download(data_generator, root="my_dataset", random_subsets={'train': 0.9, 'test': 0.1})
train_dataset = torchvision.datasets.ImageFolder(root='my_dataset/train', ...)
test_dataset = torchvision.datasets.ImageFolder(root='my_dataset/test', ...)
```

#### Tensorflow

The simpliest way to generate a `tf.data.Dataset` pipeline from a data generator is to use `tf.keras.preprocessing.image_dataset_from_directory`.
Similarily to the pytorch example, users just need to provide the root paths of the downloaded datasets.

```python
import tensorflow as tf
gbif_dl.io.download(data_generator, root="my_dataset", random_subsets={'train': 0.9, 'test': 0.1})
tf.keras.preprocessing.image_dataset_from_directory(root='my_dataset/train', label_mode="categorical", labels="inferred", *args, **kwargs)
tf.keras.preprocessing.image_dataset_from_directory(root='my_dataset/test', label_mode="categorical", labels="inferred", *args, **kwargs)
```

## FAQ

#### Q: Downloading doesn't work from inside a jupyter notebook

This is a known issue of running asyncio code from within jupyter.
Please execute these lines before using gbif-dl

```python
import nest_asyncio
nest_asyncio.apply()
```

## License

MIT
