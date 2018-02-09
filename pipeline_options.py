"""
This module receives and parses the command-line arguments necessary to run the Dataflow Job pipeline.
"""

from apache_beam.options.pipeline_options import PipelineOptions

class PipelineOptions(PipelineOptions):
    """
    A class inheriting from `PipelineOptions` that contains options required
    for running the Dataflow Job pipeline.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--gcp-project-id',
                            dest='gcp_project_id',
                            required=True,
                            help='ID of the GCP project')
        parser.add_argument('--gcp-job-name',
                            dest='gcp-job-name',
                            required=True,
                            help='The Google Cloud Platform Job Name.')
        parser.add_argument('--gcp-staging-location',
                            dest='gcp-staging-location',
                            required=True,
                            help='The staging location in GCS')
        parser.add_argument('--gcp-temp-location',
                            dest='gcp-temp-location',
                            required=True,
                            help="The temp location in GCS")
        parser.add_argument('--runner',
                            dest='runner',
                            required=True,
                            help="The type of Apache Beam runner")

