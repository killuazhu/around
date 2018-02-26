## Requirement

1. Create BigQuery bucket `post_analysis` to store result
1. Create GCS bucket as temp space for DataFlow pipeline. The bucket can be any name, but Google requires it to be global unique.
1. Create BigTable `around-post` as source

```sh
cbt createtable post
cbt createfamily post location
cbt createfamily post post
cbt read post
```
