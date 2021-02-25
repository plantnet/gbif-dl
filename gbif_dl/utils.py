"""
Utility functions
"""
import asyncio
import functools
import threading
from . import runners


def watchdog(afunc):
    """Stops all tasks if there is an error"""

    @functools.wraps(afunc)
    async def run(*args, **kwargs):
        try:
            await afunc(*args, **kwargs)
        except asyncio.CancelledError:
            return
        except Exception as err:
            print(f"exception {err}")
        asyncio.get_event_loop().stop()

    return run


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
        self.result = runners.run(self.func(*self.args, **self.kwargs))


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
        return runners.run(func(*args, **kwargs))
