# Sample Data

The CSVs are the Kaggle YouTube Trending Video Statistics dataset. Keep country filenames unchanged because the pipeline derives `region` from names such as `USvideos.csv` and `GBvideos.csv`.

The category JSON files are kept as source reference data. Terraform also uploads a combined `reference/youtube_categories.json` seed into the S3 reference prefix so the first batch does not depend on a successful external API call.
