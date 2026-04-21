"""Legacy sinks package.

The in-app LakebaseSink was removed — per-pipeline Lakebase writes now flow
through the Spark Structured Streaming consumer (``notebooks/sss_rtm_job.py``),
driven by producer notebooks generated for each pipeline's configured source.
"""

__all__: list[str] = []
