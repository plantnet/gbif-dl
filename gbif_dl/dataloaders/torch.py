try:
    import torchvision
except ImportError:
    raise ImportError("Please install PyTorch and Torchvision")

from typing import AsyncGenerator, Optional, Union, Generator, Any, Iterable, Callable

from .. import io
import os


class GBIFImageDataset(torchvision.datasets.ImageFolder):
    """GBIF Image Dataset for multi-class classification

    Args:
        root (str):
            Root path of dataset.
        generator (Optional[Union[Generator, AsyncGenerator, Iterable]]):
            Url list generator.
        download (Optional[bool], optional):
            Enable download (if root path does not exist), defaults to False
    """

    def __init__(
        self,
        root: str,
        generator: Optional[Union[Generator, AsyncGenerator, Iterable]],
        download: bool = True,
        download_args: dict = {},
        *args: Any,
        **kwargs: Any,
    ) -> None:

        self.root = os.path.expanduser(root)
        self.generator = generator

        if download and not os.path.exists(self.root):
            self.download(**download_args)

        super(GBIFImageDataset, self).__init__(root=self.root, *args, **kwargs)

    def download(self, **download_args) -> None:
        io.download(items=self.generator, root=self.root, **download_args)
