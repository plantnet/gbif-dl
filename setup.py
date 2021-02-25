from setuptools import setup, find_packages

gbif_dl_version = "0.1.0"

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="gbif-dl",
    version=gbif_dl_version,
    author="Fabian-Robert Stöter (Inria)",
    author_email="fabian-robert.stoter@inria.fr",
    url="https://github.com/plantnet/gbif-dl",
    description="Machine learning data loaders for GBIF",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    python_requires=">=3.6",
    install_requires=[
        "aiofiles>=0.6.0",
        "aiohttp>=3.7.2",
        "aiohttp-retry>=2.3",
        "aiostream>=0.4.1",
        "pygbif>=0.5.0",
        "pescador>=2.1.0",
        "python-dwca-reader",
        "filetype>=1.0.0",
        "tqdm",
        "typing-extensions; python_version < '3.8'",
    ],
    extras_require={"tests": ["pytest"], "docs": ["pdoc3"]},
    # entry_points={"console_scripts": ["gbif_dl=gbif_dl.cli:download"]},
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)