"""
Python script to Run a Beam Pipeline using Google Cloud Dataflow runner.
"""
import logging
import re

import apache_beam as beam
from apache_beam.io import range_trackers
from apache_beam.io.iobase import BoundedSource, Read, SourceBundle
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.ptransform import PTransform
import billboard

import google.cloud.language as nlp
from PyLyrics import *


logging.basicConfig()

CHART = 'hot-100'
START_DATE = '2017-12-31' # None for default (latest chart)
LAST_YEAR = 2016

class BillboardSource(BoundedSource):
    """
    A class inheriting from `apache_beam.io.iobase.BoundedSource` that creates
    a custom source of Billboard Charts from the Billboard.com website.
    """
    def __init__(self):
        """
        Initializes the `BillboardSource` class.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug("Initializing `BillboardSource` class.")


    #pylint: disable=W0613
    def get_range_tracker(self, start_position=0, stop_position=None):
        """
        Implement the method `apache_beam.io.iobase.BoundedSource.get_range_tracker`.

        `BillboardSource` uses an unsplittable range tracker, which means that a
        collection can only be read sequentially. However, the range tracker
        must still be defined.
        """
        self.logger.debug('Creating the range tracker.')
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY
        range_tracker = range_trackers.OffsetRangeTracker(0, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """
        Implements the method `apache_beam.io.iobase.BoundedSource.read`.

        Scrapes charts from the Billboard.com website via the Billboard.py.
        """
        self.logger.info('Scraping Billboard.com charts.')

        chart = billboard.ChartData(CHART, date=START_DATE)

        self.logger.info('Scraping Billboard.com %s chart data since year %s',
                         CHART, chart.previousDate[:4])

        while chart.previousDate[:4] is not None and int(chart.previousDate[:4]) > LAST_YEAR:
            self.logger.info("Scraping chart %s for year %s", CHART, chart.previousDate)

            for track in chart:
                yield (chart.previousDate[:4], track.title + ' - ' + track.artist)

            try:
                chart = billboard.ChartData(CHART, chart.previousDate)
            except Exception as return_e:
                break

    def split(self, desired_bundle_size, start_position=0, stop_position=None):
        """
        Implements method `apache_beam.io.iobase.BoundedSource.split`

        `BillboardSource` is unsplittable, so only a single source is returned.
        """
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        yield SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

class BillboardChartsRead(PTransform):
    """
    A class inheriting from `apache_beam.transforms.ptransform.PTransform` that
    reads Billboard charts from Billboard.com.
    """
    def __init__(self):
        """
        Initializes `ReadBillboardCharts`. Uses the source class `BillboardSource`.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug('Initializing `ReadFromBillboard` class.')

        super(BillboardChartsRead, self).__init__()
        self._source = BillboardSource()

    def expand(self, pcoll):
        """
        Implements method `apache_beam.transforms.ptransform.PTransform.expand`.
        """
        self.logger.info('Starting Billboard.com scrape.')
        return pcoll | Read(self._source)

    def display_data(self):
        """
        Implements method `apache_beam.transforms.ptransform.PTransform.display_data`.
        """
        return {'source_dd': self._source}

class BillboardPipeline(object):
    """
    A class that implements an abstraction on top of `beam.Pipeline`, for the
    sake of logging and monitoring.
    """
    def __init__(self, options):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Initializing `BillboardPipeline` class.")

        self.options = options
        self.pipeline = self.get_pipeline()

    #pylint: disable=R0201
    def get_sentiment_score(self, lyric_string):
        """
        Query the Google Cloud Natural Language API for a sentiment score and
        magnitude for a given String of song lyrics.

        This function receives song lyrics as a plaintext String, then uses the
        Google Cloud Natural Language API to apply sentiment analysis to the
        text. The result is a sentiment score and sentiment magnitude for the
        song.
        """
        client = nlp.LanguageServiceClient()
        document = nlp.types.Document(
            content=lyric_string,
            type=nlp.enums.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(document=document).document_sentiment

        return (sentiment.score, sentiment.magnitude)

    #pylint: disable=R0201
    def analyze_lyrics(self, element):
        """
        Analyze song lyrics to retrieve sentiment analysis scores.

        This function receives a `PCollection` object with two values:

        1. The year to analyze, and
        2. A list of song and artist names, representing a song that charted in
        the Billboard Hot-100 for the given year.

        The function then iterates over every song name in the list, retrieves
        its lyrics using the PyLyrics package, and calls the `get_sentiment_score`
        method to get the sentiment score and sentiment magnitude per song.

        The return value of this function is a String representing the  average
        sentiment score and average sentiment magnitude per year analyzed.

        For example: '2017 | 0.06 | 3'
        """
        self.logger.info('Analyzing year %s', element[0])

        year_list = list(element[1])
        score = 0
        magnitude = 0
        total = 0
        total_not_found = 0

        for index in year_list:
            try:
                song = re.split(' - ', index)
                lyrics = PyLyrics.getLyrics(song[1], song[0])
                sentiment = self.get_sentiment_score(lyrics)
                score = score + sentiment[0]
                magnitude = magnitude + sentiment[1]
                total = total + 1
            except Exception as return_e:
                total_not_found = total_not_found + 1
                self.logger.error('Could not fetch lyrics for %s: %s', index, return_e)

        if total > 0:
            avg_score_per_year = score / total
            avg_magnitude_per_year = magnitude / total

            self.logger.info('Total songs analyzed: %s', total)
            self.logger.info('Total songs not found via PyLyrics: %s', total_not_found)
            self.logger.info('Year analyzed: %s', element[0])
            output_score_string = '{} | {} | {}'.format(
                element[0],
                avg_score_per_year,
                avg_magnitude_per_year)

            self.logger.info(output_score_string)

            return output_score_string
        else:
            return 0

    def get_pipeline(self):
        """
        Creates and configures an Apache Beam pipeline to analyze song lyrics per year.
        """
        pipeline = beam.Pipeline(options=self.options)

        #pylint: disable=W0106
        (pipeline
         | BillboardChartsRead()
         | beam.GroupByKey()
         | beam.Map(self.analyze_lyrics)
         | beam.io.WriteToText('gs://billboard_charts/text_results.txt'))

        return pipeline

    def run(self):
        """
        Run the Apache Beam Pipeline.
        """
        self.pipeline.run()

def run(argv=None):
    """
    Run the Python script.

    This function receives command-line parameters to create an Apache Beam job,
    then run it--possibly on Google Cloud Dataflow. The output of this function
    is an argv vector that contains the command-line arguments with the pipeline
    options.
    """
    options = PipelineOptions(argv)
    pipeline = BillboardPipeline(options)
    pipeline.run()

if __name__ == '__main__':
    run()