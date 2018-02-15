"""
This module receives and parses the command-line arguments necessary to run the Dataflow Job pipeline.
"""

from apache_beam.options.pipeline_options import PipelineOptions

class PipelineMariaOptions(PipelineOptions):
    """
    A class inheriting from `PipelineOptions` that contains options required
    for running the Dataflow Job pipeline.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--job-name',
                            dest='job_name',
                            required=True,
                            help='The Google Cloud Platform Job Name.')
