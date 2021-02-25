"""
__gbif-dl__ provides easy access to media data from the GBIF database to be used for training machine learning models.
It wraps the GBIF API and supports directly querying the api to obtain and download a list of urls.
Existing queries can also be obtained using the download api of GBIF simply by providing a GBIF DOI key.
The package provides an efficient downloader that uses pythons modern asyncio modules to speed up downloading of
many small files as typically occur in downloading image files.

This is the python package API documentation.

Please checkout [github repository](https://github.com/plantnet/gbif-dl) for more information.
"""

from . import io
from .generators import dwca
from .generators import api
