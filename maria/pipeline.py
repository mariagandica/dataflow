"""
Python script to get all the information about all the
artist with a track in Hot-100 Billboard chart.
"""
import re
import apache_beam as beam

from pipeline_options import PipelineMariaOptions
from PyLyrics import *
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types



def query_sentiment_score(text):
    """
    Query sentiment score and magnitude from song lyrics.

    This function receives a song lyrics as plain text and then uses
    Google Cloud Natural Language library to apply sentiment analysis
    to the text.
    The input for this function is a complete lyrics of a song as
    plain text.
    The result is a sentiment score and sentiment
    magnitude per song.
    e. g. sentiment.score = 0.6, sentiment.magnitude = 3
    """
    client = language.LanguageServiceClient()
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT
        )
    sentiment = client.analyze_sentiment(document=document).document_sentiment
    return (sentiment.score, sentiment.magnitude)

def analyze(element):
    """
    Analyze element lyrics with sentiment analysis.

    """
    print element
    return element

def create_pipeline(options):
    """
    This is the code to get the pipeline up and running
    """
    pipeline = beam.Pipeline(options=options)
    (pipeline
     | beam.Create([
               'To be, or not to be: that is the question: ',
               'Whether \'tis nobler in the mind to suffer ',
               'The slings and arrows of outrageous fortune, ',
               'Or to take arms against a sea of troubles, '])
     | beam.Map(analyze)
     | beam.io.WriteToText('gs://billboard_charts/results.txt')
    )
    pipeline.run().wait_until_finish()

def run(argv=None):
    """Run the python script.
    This functions receives command-line parameters to create and run a Dataflow Job.
    The input of this funtion is argv vector that contains the command-line arguments
    with the pipeline options.
    """
    options = PipelineMariaOptions(argv)
    create_pipeline(options)

if __name__ == '__main__':
    run()
