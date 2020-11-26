from torchvision.datasets import ImageFolder
import os


class GBIFDataset(ImageFolder):
    def __init__(self, root, split, download=True, **kwargs):
        self.root = root
        self.split = split
        super().__init__(self.split_folder, **kwargs)

    @property
    def split_folder(self):
        # TODO implement split
        return os.path.join(self.root)


def create_dataset(root):
    return GBIFDataset(root=root, split=None)