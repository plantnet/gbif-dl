import asyncio
import inspect
import threading
from pathlib import Path
from typing import AsyncGenerator, Generator, List, Optional, Union
from collections.abc import Iterable

import aiofiles
import aiohttp
import aiostream
from aiohttp_retry import RetryClient, RetryOptions
from tqdm.asyncio import tqdm


def async_wrap_iter(it):
    """Wrap blocking iterator into an asynchronous one"""
    loop = asyncio.get_event_loop()
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


async def download_single_url(sample, session, root="downloads"):
    url = sample['url']

    async with session.get(url) as res:
        content = await res.read()

    # Check everything went well
    if res.status != 200:
        print(f"Download failed: {res.status}")
        return

    # check for path
    label_path = Path(root, sample['label'])
    label_path.mkdir(parents=True, exist_ok=True)
    file_path = (label_path / sample['hash']).with_suffix('.jpg')

    async with aiofiles.open(file_path, "+wb") as f:
        await f.write(content)


async def download_queue(queue, session, root="downloads"):
    while True:
        batch = await queue.get()
        for sample in batch:
            await download_single_url(sample, session, root)
        queue.task_done()


async def download(
    rows: Union[Generator, AsyncGenerator],
    root: str = "data",
    tcp_connections: int = 128,
    nb_workers: int = 128,
    batch_size: int = 8,
    retries: int = 3,
    verbose: bool = False
):
    """Asynchronous downloader that takes an interable and downloads it

    Args:
        rows (Union[Generator, AsyncGenerator]):
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
    # check if the generator is async
    if not inspect.isasyncgen(rows):
        # if its not, apply hack to make it async
        if inspect.isgenerator(rows):
            rows = async_wrap_iter(rows)
        elif isinstance(rows, Iterable):
            rows = aiostream.stream.iterate(rows)
        else:
            raise NotImplementedError(
                "Provided generator was not async and couldn't be converted"
            )

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
        async with aiostream.stream.chunks(rows, batch_size).stream() as chnk:
            async for batch in chnk:
                await queue.put(batch)
                progressbar.update(len(batch))

        await queue.join()

    for w in workers:
        w.cancel()
