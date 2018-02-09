"""
Python script to get all the information about all the artist with a track
in Hot-100 Billboard chart.
"""
import apache_beam as beam

from api_io import ReadFromAPI
from pipeline_options import PipelineOptions
from PyLyrics import *
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import re

PLAYLIST_NAME = 'Best of the Hot 100'
TRACK_COUNT = 100
CHART = 'hot-100'
START_DATE = None  # None for default (latest chart)
THROTTLE_TIME = 0.50  # Seconds

def query_sentiment_score(text):
    client = language.LanguageServiceClient()
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT
        )
    sentiment = client.analyze_sentiment(document=document).document_sentiment
    return (sentiment.score, sentiment.magnitude)

def analyze_lyrics(element):

    print "ESTOY ANALIZANDO: "+element[0]

    year_list = list(set(element[1]))
    score = 0
    magnitude = 0
    total = 0

    for index in year_list:
        try:
            song = re.split(' - ', index)
            lyrics = PyLyrics.getLyrics(song[1], song[0])
            sentiment = query_sentiment_score(lyrics)
            score = score + sentiment[0]
            magnitude = magnitude + sentiment[1]
            total = total + 1
        except:
            print "Could not fetch lyrics for: "+index

    avgScorePerYear = score / total
    avgMagnitudePerYear = magnitude / total

    print '{} | {} | {}'.format(element[0], avgScorePerYear, avgMagnitudePerYear)
    return '{} | {} | {}'.format(element[0], avgScorePerYear, avgMagnitudePerYear)

def create_pipeline(options):
    """
    This is the code to get the pipeline up and running
    """
    pipeline = beam.Pipeline(options=options)
    (pipeline
     | ReadFromAPI()
     | beam.GroupByKey()
     | beam.Map(analyze_lyrics)
     | beam.io.WriteToText('gs://pycaribbean/MariasSongs.txt')
    )
    pipeline.run()

def run(argv=None):
    """Run the python script.

    This functions receives command-line parameters to create and run a
    Dataflow Job.
    The input of this funtion is argv vector that contains the command-line
    arguments with the pipeline options.
    """
    options = PipelineOptions(argv)
    create_pipeline(options)

if __name__ == '__main__':
    run()
