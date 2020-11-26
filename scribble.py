import gbifdataset.dwca
import gbifdataset.api
from gbifdataset.torch import create_dataset

root = "./indanger_doi"
gbifdataset.dwca.get_data("10.15468/dl.vnm42s", root="indanger_doi")
train_dataset_doi = create_dataset(root)

# remove 

root = "indanger_api"
gbifdataset.api.get_data(
    queries = {
        "scientificName": [
            "Robinia pseudoacacia L",
            "Ailanthus altissima (Mill.) Swingle",
            "Acer negundo L",
        ],
        "datasetKey": [
            "7a3679ef-5582-4aaa-81f0-8c2545cafc81",  # plantnet
            # "50c9509d-22c7-4a22-a47d-8c48425ef4a7"  # inaturalist
        ]
    },
    label="speciesKey",
    balance_by=["scientificName"],
    root=root
)

train_dataset_api = create_dataset(root)
import ipdb; ipdb.set_trace()

