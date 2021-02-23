import gbif_dl

queries = {
    "speciesKey": [
        5352251,  # "Robinia pseudoacacia L"
        3190653,  # "Ailanthus altissima (Mill.) Swingle"
        3189866,  # "Acer negundo L"
    ],
    "datasetKey": ["7a3679ef-5582-4aaa-81f0-8c2545cafc81", "50c9509d-22c7-4a22-a47d-8c48425ef4a7"],
}


data_generator = gbif_dl.api.generate_urls(queries=queries, label="speciesKey", nb_samples=2)

for data in data_generator:
    print("boom")

# import filetype

# def check_mime(content):
#     # [148, 138, 145]
#     kind = filetype.guess(content)
#     print(kind.extension)
#     return True

# # [133, 160 ]

# gbif_dl.io.download(data_generator, root="test_jc2", verbose=True, overwrite=False)
# #
# import gbif_dl

# data_generator = gbif_dl.dwca.generate_urls(
#     "10.15468/dl.vnm42s", dwca_root_path="dwcas", label=None
# )
# gbif_dl.io.download(data_generator)