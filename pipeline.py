"""
Python script to get all the information about all the artist with a track
in Hot-100 Billboard chart.
"""
import re
import apache_beam as beam

from api_io import ReadFromAPI
from pipeline_options import PipelineOptions
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

def analyze_lyrics(element):
    """
    Analyze song lyrics with sentiment analysis.

    This function receives a PCollection object with
    two values: the first one is the year to analyze,
    and the second is a list of song names and artist
    that represents a song that got a position in the
    Hot-100 Billboard chart for the year to analyze.

    This function iterates over every song name in the list
    and queries its lyrics using PyLyrics library, then
    calls the function `query_sentiment_score` to get
    the sentiment score and sentiment magnitude per song.

    The return value of this functions is an average
    sentiment score and an average sentiment magnitude
    per year analyzed.
    e.g. 2017 | 0.06 | 3
    """

    print "Analyzing year: {}".format(element[0])

    year_list = list(set(element[1]))
    score = 0
    magnitude = 0
    total = 0
    total_not_found = 0

    for index in year_list:
        try:
            song = re.split(' - ', index)
            lyrics = PyLyrics.getLyrics(song[1], song[0])
            sentiment = query_sentiment_score(lyrics)
            score = score + sentiment[0]
            magnitude = magnitude + sentiment[1]
            total = total + 1
        except Exception as return_e:
            total_not_found = total_not_found + 1
            print "Could not fetch lyrics for: {} (Exception: {})".format(index, return_e.message)

    avg_score_per_year = score / total
    avg_magnitude_per_year = magnitude / total

    print "Total songs analyzed: {}".format(total)
    print "Total songs not found in PyLyrics: {}".format(total_not_found)
    print "Year analyzed: {}".format(element[0])

    print '{} | {} | {}'.format(element[0], avg_score_per_year, avg_magnitude_per_year)
    return '{} | {} | {}'.format(element[0], avg_score_per_year, avg_magnitude_per_year)

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
    pipeline.run().wait_until_finish()

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
