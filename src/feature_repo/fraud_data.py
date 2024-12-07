from pathlib import Path

from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource
from feast import types as feast_types
from feast.value_type import ValueType

auth_attempt = Entity(
    name="auth_attempt",
    value_type=ValueType.STRING,
    join_keys=["user_id"],
    description="",
)

file_sources = []
push_sources = []
feature_views = []
for i in [1, 5, 10, 15]:
    file_source = FileSource(
        path=str(Path(__file__).parent / f"./data/auth_attempt_{i}.parquet"),
        timestamp_field="timestamp",
    )
    file_sources.append(file_source)

    push_source = PushSource(
        name=f"auth_attempt_push_{i}",
        batch_source=file_source,
    )
    push_sources.append(push_source)

    feature_views.append(
        FeatureView(
            name=f"auth_attempt_view_w{i}",
            entities=[auth_attempt],
            schema=[
                Field(
                    name="user_id",
                    dtype=feast_types.String,
                ),
                Field(
                    name="timestamp",
                    dtype=feast_types.String,
                ),
                Field(
                    name=f"{i}_success",
                    dtype=feast_types.Int32,
                ),
                Field(
                    name=f"{i}_total",
                    dtype=feast_types.Int32,
                ),
                Field(
                    name=f"{i}_ratio",
                    dtype=feast_types.Float32,
                ),
            ],
            source=push_source,
            online=True,
        )
    )

auth_attempt_feature_service = FeatureService(
    name="auth_attempt_service",
    features=feature_views,
    description="Feature service for retrieving sensor statistics",
)

all_entities = (
    [
        auth_attempt,
        auth_attempt_feature_service,
    ]
    + file_sources
    + push_sources
    + feature_views
)
