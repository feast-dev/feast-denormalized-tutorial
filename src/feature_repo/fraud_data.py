from pathlib import Path

from feast import Entity, FeatureService, FeatureView, FileSource, PushSource
from feast.value_type import ValueType

from pipeline.stream_job import ds

auth_attempt = Entity(
    name="auth_attempt",
    value_type=ValueType.STRING,
    join_keys=["user_id"],
    description="",
)

auth_attempt_source = FileSource(
    path=str(Path(__file__).parent / "./data/auth_attempt.parquet"),
    timestamp_field="window_start_time",
)

auth_attempt_push_source = PushSource(
    name="push_sensor_statistics",
    batch_source=auth_attempt_source,
)

auth_attempt_view = FeatureView(
    name="auth_attempt_view",
    entities=[auth_attempt],
    schema=ds.get_feast_schema(),
    source=auth_attempt_push_source,
    online=True,
)

auth_attempt_feature_service = FeatureService(
    name="auth_attempt_service",
    features=[auth_attempt_view],
    description="Feature service for retrieving sensor statistics",
)
