try:
    import tensorflow as tf
except ImportError:
    raise ImportError("Please install Tensorflow")


from typing import Any
import os
from .. import io


def create_dataset_from_generator(
    generator, root: str, download: bool = True, download_args: dict = {}, *args: Any, **kwargs: Any
) -> tf.data.Dataset:
    """Creates tensorflow dataset from generator

    Args:
        generator (Iterable): gbif url generator
        root (str): root path to dataset
        download (bool, optional): Download files. Defaults to True.
        download_args (dict, optional): Download options for io.download(). Defaults to {}.

    Returns:
        tf.data.Dataset:
    """
    if download and not os.path.exists(root):
        io.download(items=generator, root=root, **download_args)

    return tf.keras.preprocessing.image_dataset_from_directory(
        root, label_mode="categorical", labels="inferred", *args, **kwargs
    )
