import sys
from typing import Optional

if sys.version_info >= (3, 8):
    from typing import TypedDict  # pylint: disable=no-name-in-module
else:
    from typing_extensions import TypedDict


class MediaData(TypedDict):
    """Media dict representation received from api or dwca generators"""

    url: str
    basename: Optional[str]
    label: Optional[str]
    subset: Optional[str]
    publisher: Optional[str]
    license: Optional[str]
    rightsHolder: Optional[str]
