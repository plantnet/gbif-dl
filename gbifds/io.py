import asyncio
import inspect
import threading
from pathlib import Path
from typing import AsyncGenerator, Dict, Generator, Union
from collections.abc import Iterable

import aiofiles
import aiohttp
import aiostream
from aiohttp_retry import RetryClient, RetryOptions
from tqdm.asyncio import tqdm

def async_wrap_iter(it):
    """Wrap blocking iterator into an asynchronous one"""
    loop = get_or_create_eventloop()
    q = asyncio.Queue(1)
    exception = None
    _END = object()

    async def yield_queue_items():
        while True:
            next_item = await q.get()
            if next_item is _END:
                break
            yield next_item
        if exception is not None:
            # the iterator has raised, propagate the exception
            raise exception

    def iter_to_queue():
        nonlocal exception
        try:
            for item in it:
                # This runs outside the event loop thread, so we
                # must use thread-safe API to talk to the queue.
                asyncio.run_coroutine_threadsafe(q.put(item), loop).result()
        except Exception as e:
            exception = e
        finally:
            asyncio.run_coroutine_threadsafe(q.put(_END), loop).result()

    threading.Thread(target=iter_to_queue).start()
    return yield_queue_items()


async def download_single(item: Dict, session: RetryClient, root: str = "downloads"):
    """Async function to download single url to disk

    Args:
        item (Dict): item details, including url and filename
        session (RetryClient): aiohttp session
        root (str, optional): Root path of download. Defaults to "downloads".
    """
    url = item['url']

    async with session.get(url) as res:
        content = await res.read()

    # Check everything went well
    if res.status != 200:
        print(f"Download failed: {res.status}")
        return

    # check for path
    label_path = Path(root, item['label'])
    label_path.mkdir(parents=True, exist_ok=True)
    file_path = (label_path / item['basename']).with_suffix(item['suffix'])

    async with aiofiles.open(file_path, "+wb") as f:
        await f.write(content)


async def download_queue(queue: asyncio.Queue, session: RetryClient, root: str):
    """Consumes items from download queue

    Args:
        queue (asyncio.Queue): Queue of items
        session (RetryClient): RetryClient aiohttp session object
        root (str, optional): root path.
    """
    while True:
        batch = await queue.get()
        for sample in batch:
            await download_single(sample, session, root)
        queue.task_done()


async def download_from_asyncgen(
    items: AsyncGenerator,
    root: str = "data",
    tcp_connections: int = 256,
    nb_workers: int = 256,
    batch_size: int = 16,
    retries: int = 3,
    verbose: bool = False
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
        bose (bool, if isinstance(e, Iterable):ptional): 
            Activate verbose. Defaults to False.

    Raises:
        NotImplementedError: If generator turns out to be invalid.
    """

    queue = asyncio.Queue(nb_workers)

    retry_options = RetryOptions(attempts=retries)

    async with RetryClient(
        connector=aiohttp.TCPConnector(limit=tcp_connections),
        raise_for_status=False, 
        retry_options=retry_options
    ) as session:

        workers = [
            asyncio.create_task(
                download_queue(queue, session, root=root)
            )
            for _ in range(nb_workers)
        ]

        progressbar = tqdm(smoothing=0, unit=' Images', disable=verbose)
        # get chunks from async generator
        async with aiostream.stream.chunks(items, batch_size).stream() as chnk:
            async for batch in chnk:
                await queue.put(batch)
                progressbar.update(len(batch))

        await queue.join()

    for w in workers:
        w.cancel()


def get_or_create_eventloop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError as ex:
        if "There is no current event loop in thread" in str(ex):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return asyncio.get_event_loop()

class RunThread(threading.Thread):
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        super().__init__()

    def run(self):
        self.result = asyncio.run(self.func(*self.args, **self.kwargs))

def run_async(func, *args, **kwargs):
    """async wrapper to detect if asyncio loop is already running

    This is useful when already running in async thread.
    """
    try:
        loop = get_or_create_eventloop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = RunThread(func, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(func(*args, **kwargs))

def download(
    items: Union[Generator, AsyncGenerator, Iterable],
    root: str = "data",
    tcp_connections: int = 256,
    nb_workers: int = 256,
    batch_size: int = 16,
    retries: int = 3,
    verbose: bool = False
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
        bose (bool, if isinstance(e, Iterable):ptional): 
            Activate verbose. Defaults to False.

    Raises:
        NotImplementedError: If generator turns out to be invalid.
    """

    # check if the generator is async
    if not inspect.isasyncgen(items):
        # if its not, apply hack to make it async
        if inspect.isgenerator(items):
            items = async_wrap_iter(items)
        elif isinstance(items, Iterable):
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
        verbose=verbose
    )
