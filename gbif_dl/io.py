import asyncio
import inspect
from pathlib import Path
from typing import AsyncGenerator, Callable, Generator, Union, Optional
import sys
import json
import hashlib


if sys.version_info >= (3, 8):
    from typing import TypedDict  # pylint: disable=no-name-in-module
else:
    from typing_extensions import TypedDict

from collections.abc import Iterable


import filetype
import aiofiles
import aiohttp
import aiostream
from aiohttp_retry import RetryClient, ExponentialRetry
from tqdm.asyncio import tqdm
from .utils import watchdog, run_async

class MediaData(TypedDict):
    """ Media dict representation received from api or dwca generators"""
    url: str
    basename: Optional[str]
    label: Optional[str]
    split: Optional[str]

async def download_single(
    item: MediaData,
    session: RetryClient,
    root: str = "downloads",
    is_valid_file: Optional[Callable[[bytes], bool]] = None,
    overwrite: bool = False,
    proxy: Optional[str] = None,
):
    """Async function to download single url to disk

    Args:
        item (Dict): item details, including url and filename
        session (RetryClient): aiohttp session
        root (str, optional): Root path of download. Defaults to "downloads".
        is_valid_file (optional): A function that takes bytes
            and checks if the bytes originate from a valid file
            (used to check of corrupt files). Defaults to None.
        overwrite (bool):
            overwrite files with existing `baseline` signature, Defaults to False.
        proxy (str):
            proxy server url. Authentication credentials can be passed in URL.
            e.g `proxy="http://user:pass@some.proxy.com"`.
            Proxy can also be used globally using environmental variables.
            See https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html.
    """
    if isinstance(item, dict):
        url = item.get('url')
        label = item.get('label')
        basename = item.get('basename')
        split = item.get('split')
    else:
        url = item
        label, basename, split = None, None, None

    label_path = Path(root)

    if split is not None:
        label_path /= Path(split)

    # create subfolder when label is a single str
    if isinstance(label, str):
        # append label path
        label_path /= Path(label)

    label_path.mkdir(parents=True, exist_ok=True)

    if basename is None:
        # hash the url
        basename = hashlib.sha1(
            url.encode('utf-8')
        ).hexdigest()

    check_files_with_same_basename = label_path.glob(basename + "*")
    if list(check_files_with_same_basename) and not overwrite:
        # do not overwrite, skips based on base path 
        return

    async with session.get(url, proxy=proxy) as res:
        content = await res.read()

    # guess mimetype and suffix from content
    kind = filetype.guess(content)
    if kind is None:
        print('Cannot guess file type!')
        return
    else:
        suffix = "." + kind.extension
        mime = kind.mime

    # Check everything went well
    if res.status != 200:
        print(f"Download failed: {res.status}")
        return

    if is_valid_file is not None:
        if not is_valid_file(content):
            print(f"File check failed")
            return

    file_base_path = label_path / basename
    file_path = file_base_path.with_suffix(suffix)
    async with aiofiles.open(file_path, "+wb") as f:
        await f.write(content)

    if isinstance(label, dict):
        json_path = (label_path / item['basename']).with_suffix('.json')
        async with aiofiles.open(json_path, mode='+w') as fp:
            await fp.write(json.dumps(label))

async def download_queue(
    queue: asyncio.Queue,
    session: RetryClient,
    root: str,
    is_valid_file: Optional[Callable[[bytes], bool]] = None,
    overwrite: bool = False,
    proxy: Optional[str] = None
):
    """Consumes items from download queue

    Args:
        queue (asyncio.Queue): Queue of items
        session (RetryClient): RetryClient aiohttp session object
        root (str, optional): root path.
        is_valid_file (optional): A function that takes bytes
            and checks if the bytes originate from a valid file
            (used to check of corrupt files). Defaults to None.
        overwrite (bool):
            overwrite files with existing `baseline` signature, Defaults to False.
        proxy (str):
            proxy server url. Authentication credentials can be passed in URL.
            e.g `proxy="http://user:pass@some.proxy.com"`.
            Proxy can also be used globally using environmental variables.
            See https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html.
    """
    while True:
        batch = await queue.get()
        for sample in batch:
            await download_single(
                sample,
                session,
                root,
                is_valid_file,
                overwrite,
                proxy
            )
        queue.task_done()


async def download_from_asyncgen(
    items: AsyncGenerator,
    root: str = "data",
    tcp_connections: int = 64,
    nb_workers: int = 64,
    batch_size: int = 16,
    retries: int = 3,
    verbose: bool = False,
    overwrite: bool = False,
    is_valid_file: Optional[Callable[[bytes], bool]] = None,
    proxy: Optional[str] = None
):
    """Asynchronous downloader that takes an interable and downloads it

    Args:
        items (Union[Generator, AsyncGenerator]):
            (async/sync) generator that yiels a standardized dict of urls
        root (str, optional):
            Root path of downloads. Defaults to "data".
        tcp_connections (int, optional): 
            Maximum number of concurrent TCP connections. Defaults to 128.
        nb_workers (int, optional):
            Maximum number of workers. Defaults to 128.
        batch_size (int, optional):
            Maximum queue batch size. Defaults to 8.
        retries (int, optional):
            Maximum number of retries. Defaults to 3.
        verbose (bool, if isinstance(e, Iterable):ptional): 
            Activate verbose. Defaults to False.
        overwrite (bool):
            overwrite files with existing `baseline` signature, Defaults to False.
        is_valid_file (optional): A function that takes bytes
            and checks if the bytes originate from a valid file
            (used to check of corrupt files). Defaults to None.
            overwrite existing files, Defaults to False.
        proxy (str):
            proxy server url. Authentication credentials can be passed in URL.
            e.g `proxy="http://user:pass@some.proxy.com"`.
            Proxy can also be used globally using environmental variables.
            See https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html.

    Raises:
        NotImplementedError: If generator turns out to be invalid.
    """

    queue = asyncio.Queue(nb_workers)

    retry_options = ExponentialRetry(attempts=retries)

    async with RetryClient(
        connector=aiohttp.TCPConnector(limit=tcp_connections),
        raise_for_status=False,
        retry_options=retry_options,
        trust_env=True
    ) as session:

        workers = [
            asyncio.create_task(
                download_queue(
                    queue,
                    session,
                    root=root,
                    overwrite=overwrite,
                    is_valid_file=is_valid_file,
                    proxy=proxy
                )
            )
            for _ in range(nb_workers)
        ]

        progressbar = tqdm(smoothing=0, unit=' Files', disable=verbose)
        # get chunks from async generator
        async with aiostream.stream.chunks(items, batch_size).stream() as chnk:
            async for batch in chnk:
                await queue.put(batch)
                progressbar.update(len(batch))

        await queue.join()

    for w in workers:
        w.cancel()


def download(
    items: Union[Generator, AsyncGenerator, Iterable],
    root: str = "data",
    tcp_connections: int = 128,
    nb_workers: int = 128,
    batch_size: int = 16,
    retries: int = 3,
    verbose: bool = False,
    overwrite: bool = False,
    is_valid_file: Optional[Callable[[bytes], bool]] = None,
    proxy: Optional[str] = None
):
    """Core download function that takes an interable (sync or async)

    Args:
        items (Union[Generator, AsyncGenerator, Iterable]):
            (async/sync) generator or list that yiels a standardized dict of urls
        root (str, optional):
            Root path of downloads. Defaults to "data".
        tcp_connections (int, optional): 
            Maximum number of concurrent TCP connections. Defaults to 128.
        nb_workers (int, optional):
            Maximum number of workers. Defaults to 128.
        batch_size (int, optional):
            Maximum queue batch size. Defaults to 8.
        retries (int, optional):
            Maximum number of retries. Defaults to 3.
        verbose (bool, optional): 
            Activate verbose. Defaults to False.
        overwrite (bool):
            overwrite files with existing `baseline` signature, Defaults to False.
        is_valid_file (optional): A function that takes bytes
            and checks if the bytes originate from a valid file
            (used to check of corrupt files). Defaults to None.
            overwrite existing files, Defaults to False.
        proxy (str):
            proxy server url. Authentication credentials can be passed in URL.
            e.g `proxy="http://user:pass@some.proxy.com"`.
            Proxy can also be used globally using environmental variables.
            See https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html.

    Raises:
        NotImplementedError: If generator turns out to be invalid.
    """

    # check if the generator is async
    if not inspect.isasyncgen(items):
        # if its not, apply hack to make it async
        if inspect.isgenerator(items) or isinstance(items, Iterable):
            items = aiostream.stream.iterate(items)
        else:
            raise NotImplementedError(
                "Provided iteratable could not be converted"
            )
    return run_async(
        download_from_asyncgen,
        items,
        root=root,
        tcp_connections=tcp_connections,
        nb_workers=nb_workers,
        batch_size=batch_size,
        retries=retries,
        verbose=verbose,
        overwrite=overwrite,
        is_valid_file=is_valid_file,
        proxy=proxy
    )
