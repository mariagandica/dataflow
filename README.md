# dataflow


* To run this script locally, you will need to run this command in an activated virtual environment:
```python pipeline.py --project PROJECTID --job-name mydataflowjob --staging_location gs://pycaribbean/staging_location --temp_location gs://pycaribbean/temp_location --runner DirectRunner```


* To run this script with Dataflow, you will need to run this command in an activated virtual environment:
```python pipeline.py --project PROJECTID --job-name mydataflowjob --staging_location gs://pycaribbean/staging_location --temp_location gs://pycaribbean/temp_location --runner DataflowRunner```