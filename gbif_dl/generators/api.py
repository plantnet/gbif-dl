"""
This module creates the interface to the GBIF API. By providing queries to the api, users can use 
this module to obtain lists of urls of media data to be downloaded using the [io](gbif_dl.io) module.
"""

import pygbif
import itertools as it
import random
import pescador
import hashlib
import logging
import numpy as np

from ..io import MediaData

from typing import Dict, Optional, Union, List

log = logging.getLogger(__name__)


def gbif_query_generator(
    page_limit: int = 300,
    mediatype: str = "StillImage",
    label: Optional[str] = None,
    subset: Optional[str] = None,
    *args,
    **kwargs,
) -> MediaData:
    """Performs media queries GBIF yielding url and label

    Args:
        page_limit (int, optional): GBIF api uses paging which can be modified. Defaults to 300.
        mediatype (str, optional): Sets GBIF mediatype. Defaults to 'StillImage'.
        label (str, optional): Output label name. Defaults to `None`.
        subset (str, optional): Subset name. Defaults to `None`.

    Yields:
        MediaData
    """
    offset = 0

    while True:
        resp = pygbif.occurrences.search(
            mediatype=mediatype, offset=offset, limit=page_limit, *args, **kwargs
        )

        # Iterate over request pages. Can possibly also done async
        for metadata in resp["results"]:
            # check if media key is present
            if metadata["media"]:
                # multiple media can be attached
                # select random url
                media = random.choice(metadata["media"])

                # hash the url, which later becomes the datatype
                hashed_url = hashlib.sha1(media["identifier"].encode("utf-8")).hexdigest()

                if label is not None:
                    output_label = str(metadata.get(label))
                else:
                    output_label = metadata

                yield {
                    "url": media["identifier"],
                    "basename": hashed_url,
                    "label": output_label,
                    "subset": subset,
                }

        if resp["endOfRecords"]:
            break
        else:
            offset = resp["offset"] + page_limit


def gbif_count(mediatype: str = "StillImage", *args, **kwargs) -> str:
    """Count the number of occurances from given query

    Args:
        mediatype (str, optional): [description]. Defaults to 'StillImage'.

    Returns:
        str: [description]
    """

    return pygbif.occurrences.search(limit=0, mediatype=mediatype, *args, **kwargs)["count"]


def _dproduct(dicts):
    """Returns the products of dicts"""
    return (dict(zip(dicts, x)) for x in it.product(*dicts.values()))


def generate_urls(
    queries: Dict,
    label: Optional[str] = None,
    split_streams_by: Optional[Union[str, List]] = None,
    subset_streams: Optional[Union[str, Dict]] = None,
    nb_samples_per_stream: Optional[int] = None,
    nb_samples: Optional[int] = None,
    weighted_streams: bool = False,
    cache_requests: bool = False,
    mediatype: str = "StillImage",
    verbose: bool = False,
):
    """Provides url generator from given query

    Args:
        queries (Dict): dictionary of queries supported by the GBIF api
        label (str, optional): Output label name.
            Defaults to `None` which yields all metadata.
        nb_samples (int): Limit the total number of samples retrieved from the API.
            When set to -1 and `split_streams_by` is not `None`,
            a minimum number of samples will be calculated
            from using the number of available samples per stream.
            Defaults to `None` which retrieves all samples from all streams until
            all streams are exchausted.
        nb_samples_per_stream (int): Limit the maximum number of items to be retrieved per stream.
            Defaults to `None` which retrieves all samples from stream until
            stream generator is exhausted.
        split_streams_by (Optional[Union[str, List]], optional): Stream identifiers to be balanced.
            Defaults to None.
        subset_streams (Optional[Union[str, Dict]], optional): Map certain streams into
            separate subsets, by setting the `subset` metadata. Supports a remainder
            value of `"*"` which acts as a wildcard. Defaults to None.
            E.g. `subset_streams={"train": { "speciesKey": [5352251, 3190653]},
            "test": { "speciesKey": "*" }}` will move species of 5352251 and 3190653
            into `train` whereas all other species will go into test.
        weighted_streams (int): Calculates sampling weights for all streams and applies them during
            sampling. To be combined with nb_samples not `None`.
            Defaults to `False`.
        cache_requests (bool, optional): Enable GBIF API cache.
            Can significantly improve API requests. Defaults to False.
        mediatype (str): supported GBIF media type. Can be `StillImage`, `MovingImage`, `Sound`.
            Defaults to `StillImage`.

    Returns:
        Iterable: generate-like object, that yields dictionaries
    """
    streams = []
    # set pygbif api caching
    pygbif.caching(cache_requests)

    # copy queries since we delete keys from the dict
    q = queries.copy()

    # if weighted_streams and nb_samples_per_stream is not None:
    #     raise RuntimeError("weights can only be applied when the number of samples are limited.")

    # Split queries into product of streamers
    if split_streams_by is not None:
        balance_queries = {}
        # if single string is provided, covert into list
        if isinstance(split_streams_by, str):
            split_streams_by = [split_streams_by]

        # remove balance_by from query and move to balance_queries
        for key in split_streams_by:
            balance_queries[key] = q.pop(key)

        # for each b in balance_queries, create a separate stream
        # later we control the sampling processs of these streams to balance them
        for b in _dproduct(balance_queries):
            subset = None
            # for each stream we wrap into pescador Streamers for additional features
            for key, value in b.items():
                if subset_streams is not None:
                    for x, y in subset_streams.items():
                        result = y.get(key)
                        if result is not None:
                            if isinstance(result, list):
                                for item in result:
                                    if value == item:
                                        subset = x
                            else:
                                if value == result:
                                    subset = x

                            # assign remainder class
                            if result == "*" and subset is None:
                                subset = x

            streams.append(
                pescador.Streamer(
                    pescador.Streamer(
                        gbif_query_generator,
                        label=label,
                        mediatype=mediatype,
                        subset=subset,
                        **q,
                        **b,
                    ),
                    # this makes sure that we only obtain a maximum number
                    # of samples per stream
                    max_iter=nb_samples_per_stream,
                )
            )

        if verbose:
            nb_queries = [
                gbif_count(mediatype=mediatype, **q, **b) for b in _dproduct(balance_queries)
            ]
            print(sum(nb_queries))

        # count the available occurances for each stream and select the min.
        # We only yield the minimum of streams to balance
        if nb_samples == -1:
            # calculate the miniumum number of samples available per stream
            nb_samples = min(
                [gbif_count(mediatype=mediatype, **q, **b) for b in _dproduct(balance_queries)]
            ) * len(streams)

        if weighted_streams:
            weights = np.array(
                [
                    float(gbif_count(mediatype=mediatype, **q, **b))
                    for b in _dproduct(balance_queries)
                ]
            )
            weights /= np.max(weights)
        else:
            weights = None

        mux = pescador.StochasticMux(
            streams,
            n_active=len(streams),  # all streams are always active.
            rate=None,  # all streams are balanced
            weights=weights,  # weight streams
            mode="exhaustive",  # if one stream fails it is not revived
        )
        return mux(max_iter=nb_samples)

    # else there will be only one stream, hence no balancing or sampling
    else:
        if nb_samples and nb_samples_per_stream:
            nb_samples = min(nb_samples, nb_samples_per_stream)
        print(nb_samples)
        return pescador.Streamer(
            gbif_query_generator, label=label, mediatype=mediatype, **q
        ).iterate(max_iter=nb_samples)
