import pygbif
from pathlib import Path
import random
import requests
import hashlib
import mimetypes
import re
import tempfile
import shutil

from ..io import MediaData

from dwca.read import DwCAReader

mmqualname = "http://purl.org/dc/terms/"
gbifqualname = "http://rs.gbif.org/terms/1.0/"


def dwca_generator(
    dwca_path: str,
    label: str = "speciesKey",
    type: str = 'StillImage'
) -> MediaData:
    """Yields media urls from GBIF Darwin Core Archive

    Args:
        dwca_path (str): path to darwin core zip file
        label (str, optional): Output label name. Defaults to "speciesKey".
        type (str, optional): Media type. Defaults to 'StillImage'.

    Yields:
        Dict: Item dictionary
    """
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
                "basename": hashed_url,
                "label": str(row.data.get(gbifqualname + label)),
                "content_type": content_type,
                "suffix": mimetypes.guess_extension(str(content_type)),
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
    """Validates if identifier is a valid DOI

    Args:
        identifier (str): potential doi string

    Returns:
        bool: true if identifier is a valid DOI
    """
    doi_patterns = [
        r"(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\'])\S)+)",
        r"(10.\d{4,9}/[-._;()/:A-Z0-9]+)",
        r"(10.\d{4}/\d+-\d+X?(\d+)\d+<[\d\w]+:[\d\w]*>\d+.\d+.\w+;\d)",
        r"(10.1021/\w\w\d+)",
        r"(10.1207/[\w\d]+\&\d+_\d+)"
    ]
    for pattern in doi_patterns:
        match = bool(re.match(pattern, identifier))
        if match:
            return True
    return False

def generate_urls(
    identifier: str,
    dwca_root_path=None,
    label: str = "speciesKey",
    mediatype: str = "StillImage"
):
    """Generate GBIF items from DOI or GBIF download key

    Args:
        identifier (str): doi or gbif key
        dwca_root_path (str, optional): Set root path where to store 
            Darwin Core zip files. Defaults to None, which results in
            the creation of temporary directries

    Returns:
        Iterable: item generator that yields files from generator
    """
    if _is_doi:
        key = doi_to_gbif_key(identifier)
    else:
        key = identifier

    if dwca_root_path is None:
        dwca_root_path = tempfile.mkdtemp()

    # download darwin core archive
    dwca_root_path = Path(dwca_root_path)
    dwca_root_path.mkdir(parents=True, exist_ok=True)
    dwca_path = Path(dwca_root_path, key + '.zip')
    if not dwca_path.exists():
        r = pygbif.occurrences.download_get(
            key=key,
            path=dwca_root_path
        )
        dwca_path = r['path']

    # extract media urls and return item generator
    return dwca_generator(
        dwca_path=dwca_path,
        label=label,
        mediatype=mediatype
    )