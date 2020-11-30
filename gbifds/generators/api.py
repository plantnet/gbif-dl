
import pygbif
import itertools as it
import random
import pescador
import mimetypes
import requests
import hashlib
import logging

from typing import Dict, Optional, Union, List

log = logging.getLogger(__name__)

@pescador.streamable
def gbif_query_generator(
    page_limit: int = 300,
    mediatype: str = 'StillImage',
    label: str = 'speciesKey',

    *args, **kwargs
) -> str:
    """Performs media queries GBIF yielding url and label

    Args:
        page_limit (int, optional): GBIF api uses paging which can be modified. Defaults to 300.
        mediatype (str, optional): Sets GBIF mediatype. Defaults to 'StillImage'.
        label (str, optional): Sets label. Defaults to 'speciesKey'.

    Returns:
        str: [description]

    Yields:
        Iterator[str]: [description]
    """
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
    """Count the number of occurances from given query

    Args:
        mediatype (str, optional): [description]. Defaults to 'StillImage'.

    Returns:
        str: [description]
    """

    return pygbif.occurrences.search(
        limit=0,
        mediatype=mediatype,
        *args, **kwargs
    )['count']


def dproduct(dicts):
    """Returns the products of dicts
    """
    return (dict(zip(dicts, x)) for x in it.product(*dicts.values()))


def get_items(
    queries: Dict,
    label: str = "speciesKey",
    balance_by: Optional[Union[str, List]] = None,
    cache_requests: bool = False
):
    """Provides url generator from given query

    Args:
        queries (Dict): dictionary of queries supported by the GBIF api
        label (str, optional): label identfier, according to query api. Defaults to "speciesKey".
        balance_by (Optional[Union[str, List]], optional): identifiers to be balanced by. Defaults to None.
        cache_requests (bool, optional): Enable GBIF API cache. Defaults to False.

    Returns:
        [type]: [description]
    """
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

    return mux(max_iter=min_count * len(streams))


