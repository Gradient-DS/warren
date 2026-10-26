"""
Framework-level constants for the worker system.
"""

PUBLISHER_ORIGIN_TYPE: str = "publisher"
"""Value for ``origin.type`` on messages that a publisher puts into the pipeline.

``RoutingPlanRouter`` routes a message whose origin is not a plan node
to the plan entry. ``JobStatusWorker`` stores the value as
``origin_type`` in the job results record. No other framework code
reads this value.
"""
