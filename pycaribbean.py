"""
Python script to get all the information about all the artist with a track
in Hot-100 Billboard chart.
"""
import apache_beam as beam

from api_io import ReadFromAPI
from pipeline_options import PipelineExampleOptions

PLAYLIST_NAME = 'Best of the Hot 100'
TRACK_COUNT = 100
CHART = 'hot-100'
START_DATE = None  # None for default (latest chart)
THROTTLE_TIME = 0.50  # Seconds

SPOTIFY_CLIENT_ID = 'b343ebb9cda7460bbd78240fdc005065'
SPOTIFY_CLIENT_SECRET = 'fefda63106c643209865698cc18dbeb5'
SPOTIFY_USERNAME = 'mariagandica13@gmail.com'
SPOTIFY_REDIRECT_URI = 'https://github.com/guoguo12/billboard-charts'

def modify_data(element):
    """
    dosctring
    """
    print element
    return element

def create_pipeline(options):
    """
    This is the code to get the pipeline up and running
    """
    pipeline = beam.Pipeline(options=options)
    (pipeline
     | ReadFromAPI(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET,
                   SPOTIFY_USERNAME, SPOTIFY_REDIRECT_URI)
     | beam.Map(modify_data)
     | beam.io.WriteToText('gs://pycaribbean/outputData.txt')
    )
    pipeline.run()

def run(argv=None):
    """Run the python script.

    This functions receives command-line parameters to create and run a
    Dataflow Job.
    The input of this funtion is argv vector that contains the command-line
    arguments with the pipeline options.
    """
    options = PipelineExampleOptions(argv)
    print options
    create_pipeline(options)

if __name__ == '__main__':
    run()
