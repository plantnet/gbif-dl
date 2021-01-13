try:
    import torchvision
except ImportError:
    raise ImportError('Please install PyTorch and Torchvision')

from typing import AsyncGenerator, Optional, Union, Generator, Any, Iterable

from .. import io
import os


class GBIFImageDataset(torchvision.datasets.ImageFolder):
    """GBIF Image Dataset for multi-class classification

    Args:
        root (str): 
            Root path of dataset
        generator (Optional[Union[Generator, AsyncGenerator, Iterable]]):
            Url list generator.
        download (Optional[bool], optional): 
            Enable download (if root path does not exist)
    """
    def __init__(
        self,
        root: str,
        generator: Optional[Union[Generator, AsyncGenerator, Iterable]],
        download: bool = True, 
        **kwargs: Any
    ) -> None:

        self.root = os.path.expanduser(root)

        if download and not os.path.exists(self.root):
            self.download(generator)
            # TODO check integrity

        super(GBIFImageDataset, self).__init__(root=self.root, **kwargs)

    def download(self, generator) -> None:
        io.download(items=generator, root=self.root)
