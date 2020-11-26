import ipdb
import pygbif
from pathlib import Path
import random
import requests
import hashlib
import mimetypes
import asyncio
import re
import tempfile

from . import api

from dwca.read import DwCAReader
from dwca.darwincore.utils import qualname as qn

mmqualname = "http://purl.org/dc/terms/"
gbifqualname = "http://rs.gbif.org/terms/1.0/"


def dwca_generator(
    dwca_path: str,
    label: str = "speciesKey",
    type: str = 'StillImage'
):
    with DwCAReader(dwca_path) as dwca:
        for row in dwca:
            img_extensions = []
            for ext in row.extensions:
                # multiple images are handled as multiple extensions
                # therefore lets filter the images first and then 
                # yield a random one
                if ext.rowtype == gbifqualname + 'Multimedia':
                    if ext.data[mmqualname + 'type'] == type:
                        img_extensions.append(ext.data)

            selected_img = random.choice(
                img_extensions
            )

            url = selected_img[mmqualname + 'identifier']

            if selected_img.get(mmqualname + 'format') is None:
                h = requests.head(url)
                header = h.headers
                content_type = header.get('content-type')
            else:
                content_type = selected_img[mmqualname + 'format']

            # hash the url, which later becomes the datatype
            hashed_url = hashlib.sha1(
                url.encode('utf-8')
            ).hexdigest()

            yield {
                "url": url,
                "hash": hashed_url,
                "label": str(row.data.get(gbifqualname + label)),
                "content_type": content_type,
                "extension": mimetypes.guess_extension(str(content_type)),
            }


def doi_to_gbif_key(doi: str) -> str:
    """get gbif download id from doi

    Args:
        doi (str): doi string (not full url)

    Returns:
        str: gbif id
    """
    r = requests.get('https://api.datacite.org/dois/' + doi)
    if r.status_code == requests.codes.ok:
        gbif_url = r.json().get('data').get('attributes').get('url')
        if gbif_url is not None:
            gbif_key = gbif_url.split('/')[-1]
            if bool(re.match('^[0-9\-]*$', gbif_key)):
                return gbif_key

def _is_doi(identifier: str) -> bool:
    """validates if str is a valid doi

    Args:
        identifier (str): potential doi string

    Returns:
        bool: true if identifier is a valid DOI
    """
    return True
    # return bool(re.match('/^10.\d{4,9}/[-._;()/:A-Z0-9]+$/i', identifier))

def get_data(identifier: str, root: str, dwca_root_path="dwcas"):
    if _is_doi:
        key = doi_to_gbif_key(identifier)
    else:
        key = identifier

    is_temp = False
    if root is None:
        # TODO: tempdir logic
        is_temp = True
        root = tempfile.mkdtemp()

    # download darwin core archive
    # TODO: check if path already exists
    dwca_root_path = Path(dwca_root_path)
    dwca_root_path.mkdir(parents=True, exist_ok=True)
    r = pygbif.occurrences.download_get(
        key=key,
        path=dwca_root_path
    )
    dwca_path = r['path']

    # extract urls images
    gen = dwca_generator(dwca_path=dwca_path)
    # download urls
    asyncio.run(api.download(generator=gen, root=root))
    return root

# TODO: use usr_data for tmp
# TODO: enable delete = True
