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


	python pipeline.py --project PROJECTID --job-name test1 --staging_location gs://billboard_charts/staging_location --temp_location gs://billboard_charts/temp_location --runner DataflowRunner --setup_file path/to/setup.py --save_main_session True

##Notes

- With the current code in pipeline.py, the job will succeed in Dataflow.
- If I change the `analyze` function un pipeline.py to:


	```sentiment = query_sentiment_score(element)```
	```print '{} | {} | {}'.format(element, sentiment[0], sentiment[1])```
	```return '{} | {} | {}'.format(element, sentiment[0], sentiment[1])```


It will return the following error:


	(4a0e1aa7332bc98a): Traceback (most recent call last):
	  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 706, in run
	    self._load_main_session(self.local_staging_directory)
	  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 446, in _load_main_session
	    pickler.load_session(session_file)
	  File "/usr/local/lib/python2.7/dist-packages/apache_beam/internal/pickler.py", line 247, in load_session
	    return dill.load_session(file_path)
	  File "/usr/local/lib/python2.7/dist-packages/dill/dill.py", line 363, in load_session
	    module = unpickler.load()
	  File "/usr/lib/python2.7/pickle.py", line 858, in load
	    dispatch[key](self)
	  File "/usr/lib/python2.7/pickle.py", line 1090, in load_global
	    klass = self.find_class(module, name)
	  File "/usr/local/lib/python2.7/dist-packages/dill/dill.py", line 423, in find_class
	    return StockUnpickler.find_class(self, module, name)
	  File "/usr/lib/python2.7/pickle.py", line 1124, in find_class
	    __import__(module)
	ImportError: No module named PyLyrics.functions

