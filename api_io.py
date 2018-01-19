"""
This module contains classes and methods inheriting from
iobase.BoundedSource to create a new custom source foy the
pipeline to read from MySQL.
"""

__all__ = ['ReadFromAPI']

import sys
import logging
import billboard
import spotipy
import spotipy.util

from apache_beam.io import iobase, range_trackers
from apache_beam.transforms import PTransform

LOGGER = logging.getLogger()
CHART = 'hot-100'
START_DATE = None  # None for default (latest chart)

class APISource(iobase.BoundedSource):
    """
    A class inheriting `apache_beam.io.iobase.BoundedSource` for creating a
    custom source for MySQL.
    """
    def __init__(self, client_id, client_secret, username, redirect_uri):
        """
        Initializes class: `MySQLSource` with the input data.
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

        Reads from custom MySQL source.
        """
        if not self._token:
            sys.exit('Authorization failed')

        spotify = spotipy.Spotify(auth=self._token)
        chart = billboard.ChartData(CHART, date=START_DATE)

        for track in chart:
            print track.artist
            results = spotify.search(q=track.artist, limit=20)
            for i, track_result in enumerate(results['tracks']['items']):
                print track_result['name']
                yield track_result['name']


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
    A class ininheriting from `apache_beam.transforms.ptransform.PTransform` for reading from MySQL
    and transform the result.
    """
    def __init__(self, client_id, client_secret, username, redirect_uri):
        """
        Initializes :class:`ReadFromMySQL`. Uses source class:`MySQLSource`
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
