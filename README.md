# dataflow

## Creating a Virtual Environment

Google Cloud Dataflow requires Python 2.7. To create a virtual environment using Python 2.7, run

```
virtualenv --python=/usr/bin/python2.7 venv
```

Then, run

```
source venv/bin/activate
```

to activate the virtual environment, and run

```
pip install -r requirements.txt
```

to install the requirements.

## Running Locally

To run this script locally, you will need to run this command in your activated virtual environment:

```
python pipeline.py --project PROJECTID --job-name mydataflowjob --staging_location gs://pycaribbean/staging_location --temp_location gs://pycaribbean/temp_location --runner DirectRunner
```

## Running on Google Cloud Dataflow

To run this script with Dataflow, you will need to run this command in your activated virtual environment:

```
python pipeline.py --project PROJECTID --job-name test1 --staging_location gs://billboard_charts/staging_location --temp_location gs://billboard_charts/temp_location --runner DataflowRunner --requirements_file requirements.txt --setup_file path/to/setup.py --extra_package path/to/billboard.py-4.2.0.tar.gz
```
