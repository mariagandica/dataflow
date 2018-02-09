"""
This module contains classes and methods inheriting from
iobase.BoundedSource to create a new custom source foy the
pipeline to read from an API.
"""

__all__ = ['ReadFromAPI']

import logging
import billboard
import spotipy
import spotipy.util

from apache_beam.io import iobase, range_trackers
from apache_beam.transforms import PTransform

LOGGER = logging.getLogger()
CHART = 'hot-100'
START_DATE = "2017-12-31"  # None for default (latest chart)
LAST_YEAR = 2016

class APISource(iobase.BoundedSource):
    """
    A class inheriting `apache_beam.io.iobase.BoundedSource` for creating a
    custom source for an API.
    """
    def __init__(self, client_id, client_secret, username, redirect_uri):
        """
        Initializes class: `APISource` with the input data.
        """
        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._redirect_uri = redirect_uri

        self._token = spotipy.util.prompt_for_user_token(
            self._username,
            scope='playlist-modify-public',
            client_id=self._client_id,
            client_secret=self._client_secret,
            redirect_uri=self._redirect_uri)

    def get_range_tracker(self, start_position=0, stop_position=None):
        """
        Implements class: `apache_beam.io.iobase.BoundedSource.get_range_tracker`

        This class uses an unsplittable range tracker. This means that a
        collection can only be read sequentially. However, the ranger must be
        defined.

        """
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY
        range_tracker = range_trackers.OffsetRangeTracker(0, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """
        Override method `read`

        Reads from custom API source.
        """

        chart = billboard.ChartData(CHART, date=START_DATE)
        print "Hola: "+chart.previousDate[:4]
        while int(chart.previousDate[:4]) > LAST_YEAR:
            print "Analizando chart "+chart.previousDate+"..."
            for track in chart:
                #dictionary = {chart.previousDate[:4]: track.title+" - "+track.artist}
                tup1 = (chart.previousDate[:4], track.title+" - "+track.artist)
                #print dictionary
                #yield dictionary
                yield tup1
            try:
                chart = billboard.ChartData('hot-100', chart.previousDate)
            except:
                break

    def split(self, desired_bundle_size, start_position=0, stop_position=None):
        """
        Implements class: `apache_beam.io.iobase.BoundedSource.split`

        Because the source is unsplittable, only a single source is
        returned.
        """
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY
        yield iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)


class ReadFromAPI(PTransform):
    """
    A class ininheriting from `apache_beam.transforms.ptransform.PTransform` for reading from an API
    and transform the result.
    """
    def __init__(self, client_id, client_secret, username, redirect_uri):
        """
        Initializes :class:`ReadFromAPI`. Uses source class:`APISource`
        """
        super(ReadFromAPI, self).__init__()
        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._redirect_uri = redirect_uri
        self._source = APISource(
            self._client_id,
            self._client_secret,
            self._username,
            self._redirect_uri)

    def expand(self, pcoll):
        """
        Implements class: `apache_beam.transforms.ptransform.PTransform.expand`
        """
        print 'Starting Spotify API read'
        LOGGER.setLevel(logging.INFO)
        LOGGER.info('Starting Spotify API read')
        return pcoll | iobase.Read(self._source)

    def display_data(self):
        """
        Override method `display_data`
        """
        return {'source_dd': self._source}
