"""
Python script to get all the information about all the artist with a track
in Hot-100 Billboard chart.
"""
import sys
import billboard
import spotipy
import spotipy.util
import apache_beam as beam

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

def get_artists():
    """
    Main function of the script.
    This function generates a new .txt file containing
    the list with all the artists.
    """
    token = spotipy.util.prompt_for_user_token(
        SPOTIFY_USERNAME,
        scope='playlist-modify-public',
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri='https://github.com/guoguo12/billboard-charts')
    if not token:
        sys.exit('Authorization failed')
    spotify = spotipy.Spotify(auth=token)
    chart = billboard.ChartData(CHART, date=START_DATE)
    file = open("mariaspycaribbean.txt","w")
    for track in chart:
        print track.artist
        results = spotify.search(q=track.artist, limit=20)
        for i, track_result in enumerate(results['tracks']['items']):
            print(' ', i, track_result['name'])
            file.write(track_result['name'].encode('utf-8').strip() + "\n")
    file.close()

def modify_data(element):
    print element

def create_pipeline(options):
    """
    This is the code to get the pipeline up and running
    """
    pipeline = beam.Pipeline(options=options)
    (pipeline
     | beam.io.ReadFromText('gs://pycaribbean/mariaspycaribbean.txt')
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
    get_artists()
    options = PipelineExampleOptions(argv)
    print options
    create_pipeline(options)

if __name__ == '__main__':
    run()
