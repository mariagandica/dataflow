"""
This module contains classes and methods inheriting from
iobase.BoundedSource to create a new custom source foy the
pipeline to read from a Billboard Library.
"""

__all__ = ['ReadBillboardCharts']

import logging
import billboard

from apache_beam.io import iobase, range_trackers
from apache_beam.transforms import PTransform

logging.basicConfig()

CHART = 'hot-100'
START_DATE = "2017-12-31"  # None for default (latest chart)
LAST_YEAR = 2016

class BillboardSource(iobase.BoundedSource):
    """
    A class inheriting `apache_beam.io.iobase.BoundedSource` for creating
    a custom source for Billboard Charts.
    """
    def __init__(self):
        """
        Initializes class: `BillboardSource` with the input data.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug("Initializing BillboardSource class...")

    def get_range_tracker(self, start_position=0, stop_position=None):
        """
        Implements class: `apache_beam.io.iobase.BoundedSource.get_range_tracker`

        This class uses an unsplittable range tracker. This means that a
        collection can only be read sequentially. However, the ranger must be
        defined.

        """
        self.logger.debug("Getting range tracker...")
        stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY
        range_tracker = range_trackers.OffsetRangeTracker(0, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """
        Override method `read`

        Reads from Billboard Library.
        """
        self.logger.info("Scraping Billboard data...")

        chart = billboard.ChartData(CHART, date=START_DATE)

        self.logger.info("Scraping data since year %s...", chart.previousDate[:4])

        while chart.previousDate[:4] is not None and int(chart.previousDate[:4]) > LAST_YEAR:
            self.logger.info("Scraping chart %s...", chart.previousDate)
            for track in chart:
                tup1 = (chart.previousDate[:4], track.title+" - "+track.artist)
                yield tup1
            try:
                chart = billboard.ChartData('hot-100', chart.previousDate)
            except Exception as return_e:
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


class ReadBillboardCharts(PTransform):
    """
    A class inheriting from `apache_beam.transforms.ptransform.PTransform`
    for reading from Billboard charts and transform the result.
    """
    def __init__(self):
        """
        Initializes :class:`ReadBillboardCharts`. Uses source class:`BillboardSource`
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Initializing ReadFromBillboard class...")
        super(ReadBillboardCharts, self).__init__()
        self._source = BillboardSource()

    def expand(self, pcoll):
        """
        Implements class: `apache_beam.transforms.ptransform.PTransform.expand`
        """
        self.logger.info('Starting Billboard scrape...')
        return pcoll | iobase.Read(self._source)

    def display_data(self):
        """
        Override method `display_data`
        """
        return {'source_dd': self._source}
