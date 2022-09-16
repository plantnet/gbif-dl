"""
Async based fast downloader.
"""
import asyncio
import inspect
from pathlib import Path
from typing import AsyncGenerator, Callable, Generator, Union, Optional
import sys
import json
import hashlib
import random
import logging
from tqdm.contrib.logging import logging_redirect_tqdm

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
from tqdm.asyncio import tqdm, tqdm_asyncio
from ..utils import run_async
from . import MediaData


class DownloadParams(TypedDict):
    root: str
    overwrite: bool
    is_valid_file: Optional[Callable[[bytes], bool]]
    proxy: Optional[str]
    random_subsets: Optional[dict]


async def download_single(
    item: Union[MediaData, str], session: RetryClient, params: DownloadParams
):
    """Async function to download single url to disk

    Args:
        item (Dict or str): item dict or url.
        session (RetryClient): aiohttp session.
        params (DownloadParams): Download parameter dict
    """
    if isinstance(item, dict):
        url = item.get("url")
        basename = item.get("basename")
        label = item.get("label")
        subset = item.get("subset")
    else:
        url = item
        label, basename, subset = None, None, None

    if subset is None and params["random_subsets"] is not None:
        subset_choices = list(params["random_subsets"].keys())
        p = list(params["random_subsets"].values())
        subset = random.choices(subset_choices, weights=p, k=1)[0]

    label_path = Path(params["root"])

    if subset is not None:
        label_path /= Path(subset)

    # create subfolder when label is a single str
    if isinstance(label, str):
        # append label path
        label_path /= Path(label)

    label_path.mkdir(parents=True, exist_ok=True)

    if basename is None:
        # hash the url
        basename = hashlib.sha1(url.encode("utf-8")).hexdigest()

    check_files_with_same_basename = label_path.glob(basename + "*")
    if list(check_files_with_same_basename) and not params["overwrite"]:
        # do not overwrite, skips based on base path
        return False

    async with session.get(url, proxy=params["proxy"]) as res:
        content = await res.read()

    # guess mimetype and suffix from content
    kind = filetype.guess(content)
    if kind is None:
        return False
    else:
        suffix = "." + kind.extension
        mime = kind.mime

    # Check everything went well
    if res.status != 200:
        raise aiohttp.ClientResponseError

    if params["is_valid_file"] is not None:
        if not params["is_valid_file"](content):
            print(f"File check failed")
            return False

    file_base_path = label_path / basename
    file_path = file_base_path.with_suffix(suffix)
    async with aiofiles.open(file_path, "+wb") as f:
        await f.write(content)

    if isinstance(label, dict):
        json_path = (label_path / item["basename"]).with_suffix(".json")
        async with aiofiles.open(json_path, mode="+w") as fp:
            await fp.write(json.dumps(label))

    return True


async def _download_queue(
    queue: asyncio.Queue,
    session: RetryClient,
    stats: dict,
    params: DownloadParams,
    progressbar: tqdm_asyncio = None,
    logger: logging.Logger = None,
):
    """Consumes items from download queue

    Args:
        queue (asyncio.Queue): Queue of items
        session (RetryClient): RetryClient aiohttp session object
        params (DownloadParams): Download parameter dict
        logger (logging.Logger): Logger object
    """
    while True:
        batch = await queue.get()
        for sample in batch:
            failed = False
            try:
                success = await download_single(sample, session, params)
            except Exception as e:
                with logging_redirect_tqdm(loggers=[logger]):
                    logger.error(e.request_info.url, extra={"status": e.status})
                    failed = True

            if failed:
                stats["failed"] += 1
            elif not success:
                stats["skipped"] += 1
            else:
                stats["success"] += 1

            progressbar.set_postfix(stats=stats, refresh=True)
            progressbar.update(1)

        queue.task_done()


async def _download_from_asyncgen(
    items: AsyncGenerator,
    params: DownloadParams,
    tcp_connections: int = 64,
    nb_workers: int = 64,
    batch_size: int = 16,
    retries: int = 1,
    logger: logging.Logger = None,
):
    """Asynchronous downloader that takes an interable and downloads it

    Args:
        items (Union[Generator, AsyncGenerator]): (async/sync) generator that yiels a standardized dict of urls
        params (DownloadParams): Download parameter dict
        tcp_connections (int, optional): Maximum number of concurrent TCP connections. Defaults to 128.
        nb_workers (int, optional): Maximum number of workers. Defaults to 64.
        batch_size (int, optional): Maximum queue batch size. Defaults to 16.
        retries (int, optional): Maximum number of attempts. Defaults to 1.
        logger (logging.Logger, optional): Logger object. Defaults to None.
    Raises:
        NotImplementedError: If generator turns out to be invalid.
    """

    queue = asyncio.Queue(nb_workers)
    progressbar = tqdm(
        smoothing=0, unit=" Downloads", disable=logger.getEffectiveLevel() > logging.INFO
    )
    stats = {"failed": 0, "skipped": 0, "success": 0}

    retry_options = ExponentialRetry(attempts=retries)

    async with RetryClient(
        connector=aiohttp.TCPConnector(limit=tcp_connections),
        raise_for_status=True,
        retry_options=retry_options,
        trust_env=True,
    ) as session:

        loop = asyncio.get_event_loop()
        workers = [
            loop.create_task(
                _download_queue(
                    queue, session, stats, params=params, progressbar=progressbar, logger=logger
                )
            )
            for _ in range(nb_workers)
        ]

        # get chunks from async generator and add to async queue
        async with aiostream.stream.chunks(items, batch_size).stream() as chnk:
            async for batch in chnk:
                await queue.put(batch)

        await queue.join()

    for w in workers:
        w.cancel()

    return stats


def download(
    items: Union[Generator, AsyncGenerator, Iterable, Path],
    root: str = "data",
    tcp_connections: int = 128,
    nb_workers: int = 128,
    batch_size: int = 16,
    retries: int = 1,
    loglevel: str = "INFO",
    error_log_path: Path = None,
    overwrite: bool = False,
    is_valid_file: Optional[Callable[[bytes], bool]] = None,
    proxy: Optional[str] = None,
    random_subsets: Optional[dict] = None,
):
    """Core download function that takes an interable (sync or async)

    Args:
        items (Union[Generator, AsyncGenerator, Iterable, Path]): (async/sync) generator
            list or path to text file that includes urls and optional labels.
            The text file should have one url per line and optional data after a whitespace.
        root (str, optional): Root path of downloads. Defaults to "data".
        tcp_connections (int, optional): Maximum number of concurrent TCP connections. Defaults to 128.
        nb_workers (int, optional): Maximum number of workers. Defaults to 128.
        batch_size (int, optional): Maximum queue batch size. Defaults to 8.
        retries (int, optional): Maximum number of attempts. Defaults to 1, which means one try.
        loglevel (str, optional): Set logger logging level.
            This shows failed downloads and a progressbar. Setting it to `ERROR` disables the progressbar.
            Setting it to `CRITICAL` disables all logging. Defaults to `INFO`.
        error_log_path (Path, optional): Writes errors to file. Defaults to None.
        overwrite (bool): overwrite files with existing `baseline` signature, Defaults to False.
        is_valid_file (optional): A function that takes bytes
            and checks if the bytes originate from a valid file
            (used to check of corrupt files). Defaults to None.
            overwrite existing files, Defaults to False.
        proxy (str): Proxy server url. Authentication credentials can be passed in URL. e.g
            `proxy="http://user:pass@some.proxy.com"`. Proxy can also be used globally using environmental variables.
            See https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html.
        random_subsets (dict[str, float]): add random subset given as a dict of class names and it's propability.
            e.g. `{'train': 0.9, test': 0.1}` will result in 90% of the items
            go into a `train` subfolder and 10% into a `test` subfolder.
            The propabilities have to sum up to `1.0` to avoid an error.

    Returns:
        dict: A dict of download statistics.

    Raises:
        NotImplementedError: If generator turns out to be invalid.
    """

    if isinstance(items, (Path, str)):
        if Path(items).exists():
            # ignore all string after first space
            items = [l.split(" ")[0] for l in Path(items).read_text().splitlines()]

    # check if the generator is async
    if not inspect.isasyncgen(items):
        # if its not, apply hack to make it async
        if inspect.isgenerator(items) or isinstance(items, Iterable):
            items = aiostream.stream.iterate(items)
        else:
            raise NotImplementedError("Provided iteratable could not be converted")

    if random_subsets is not None:
        p = random_subsets.values()
        if sum(p) != 1.0:
            raise RuntimeError("Make sure that weight probabilities add up to one")

    logger = logging.getLogger("error_urls")

    # set log format suitable for error logs and io
    formatter = logging.Formatter("%(message)s %(status)s")
    # set default log level to only receive errors
    logger.setLevel(loglevel)

    handlers = []
    if logger.getEffectiveLevel() <= logging.ERROR:
        # write errors to std.out
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        handlers.append(sh)
        # in case an error path is set, also write errors to file
        if isinstance(error_log_path, str):
            fh = logging.FileHandler(error_log_path)
            fh.setFormatter(formatter)
            handlers.append(fh)

    for handler in handlers:
        logger.addHandler(handler)

    logger.propagate = False

    params = {
        "root": root,
        "overwrite": overwrite,
        "is_valid_file": is_valid_file,
        "proxy": proxy,
        "random_subsets": random_subsets,
    }

    return run_async(
        _download_from_asyncgen,
        items,
        tcp_connections=tcp_connections,
        nb_workers=nb_workers,
        batch_size=batch_size,
        retries=retries,
        logger=logger,
        params=params,
    )
