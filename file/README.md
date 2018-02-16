# Sentiment Analysis of Billboard charts songs per year

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

## Running on Google Cloud Dataflow

To run this script with Dataflow, you will need to run this command in your activated virtual environment:

```
python pipeline.py --requirements_file requirements.txt --extra_package billboard.py-4.2.0.tar.gz --project <PROJECT-NAME> --job_name <JOB-NAME> --staging_location gs://<BUCKET-NAME>/staging_location --temp_location gs://<BUCKET-NAME>/temp_location --runner DataflowRunner --save_main_session True
```