
import pygbif
import itertools as it
import random
import pescador
import mimetypes
import requests
import hashlib
from . import io
import logging
import asyncio
import aiostream

from typing import Dict, Optional, Union, List


log = logging.getLogger(__name__)

# TODO: add resume
# TODO: support universal file queue
# TODO: speed up queries
# TODO: report statistics (downloads, )
# TODO: add unit tests
# TODO: create webdataset

@pescador.streamable
def gbif_query_generator(
    page_limit: int = 300,
    mediatype: str = 'StillImage',
    label: str = 'speciesKey',
    *args, **kwargs
) -> str:

    offset = 0

    while True:
        resp = pygbif.occurrences.search(
            mediatype=mediatype,
            offset=offset,
            limit=page_limit,
            *args, **kwargs
        )
        if resp['endOfRecords']:
            break
        else:
            offset = resp['offset'] + page_limit

        # TODO: try random offset to shuffle responses

        # Iterate over request pages. Can possibly also done async
        for metadata in resp['results']:
            # check if media key is present
            if metadata['media']:
                # multiple media can be attached
                # select random url
                media = random.choice(metadata['media'])
                # in case the media format is not determined,
                # we need to make another request to the webserver
                # to get the content-type header
                if media['format'] is None:
                    h = requests.head(media['url'])
                    header = h.headers
                    content_type = header.get('content-type')
                else:
                    content_type = media['format']

                # hash the url, which later becomes the datatype
                hashed_url = hashlib.sha1(
                    media['identifier'].encode('utf-8')
                ).hexdigest()

                # TODO: add test for failed download
                # if random.randint(0, 1):
                #     media['identifier'] = "http:////"
                yield {
                    "url": media['identifier'],
                    "basename": hashed_url,
                    "label": str(metadata.get(label)),
                    "content_type": content_type,
                    "suffix": mimetypes.guess_extension(str(content_type)),
                }


def gbif_count(
    mediatype: str = 'StillImage',
    *args, **kwargs
) -> str:

    return pygbif.occurrences.search(
        limit=0,
        mediatype=mediatype,
        *args, **kwargs
    )['count']


def dproduct(dicts):
    return (dict(zip(dicts, x)) for x in it.product(*dicts.values()))


def get_urls(
    queries: Dict,
    label: str = "speciesKey",
    balance_by: Optional[Union[str, List]] = None,
    root: str = "data",
    cache_requests: bool = False
):
    streams = []
    pygbif.caching(cache_requests)

    # use multiple queries for balancing
    if balance_by is not None:
        balance_queries = {}
        if isinstance(balance_by, str):
            balance_by = [balance_by]

        # remove balance_by from query and move to balance_queries
        for key in balance_by:
            balance_queries[key] = queries.pop(key)

        # for each b in balance_queries, create a separate stream
        # this control the sampling processs of that stream
        # thus balancing it
        for b in dproduct(balance_queries):
            streams.append(
                gbif_query_generator(
                    label=label, **queries, **b)
            )
        # count the available occurances for each stream and select the minimum
        # we will only yield the minimum of streams to balance
        min_count = min(
            [
                gbif_count(**queries, **b) for b in dproduct(balance_queries)
            ]
        )

    # else there will be only one stream, no balancing
    else:
        streams = [gbif_query_generator(label=label, **queries)]
        min_count = gbif_count(**queries)

    mux = pescador.StochasticMux(
        streams,
        n_active=len(streams),  # all streams are always active.
        rate=None,  # all streams are balanced
        mode="exhaustive"  # if one stream is empty fails we are done
    )

    gen = mux(max_iter=min_count * len(streams))
    return gen


if __name__ == "__main__":
    queries = {
        "scientificName": [
            "Robinia pseudoacacia L",
            "Ailanthus altissima (Mill.) Swingle",
            "Acer negundo L",
        ],
        "datasetKey": [
            "7a3679ef-5582-4aaa-81f0-8c2545cafc81",  # plantnet
            # "50c9509d-22c7-4a22-a47d-8c48425ef4a7"  # inaturalist
        ]
    }

    get_data(
        queries=queries,
        label="speciesKey",
        balance_by=["scientificName"],
        root="dataset"
    )



