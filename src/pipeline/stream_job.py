import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from denormalized import Context, DataStream, FeastDataStream
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit
from feast import FeatureStore
from feast.data_source import PushMode

from session_generator.login_attempt import LoginAttempt

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fake_sessions"


def print_batch(rb):
    if len(rb):
        print(rb.to_pandas())


def create_pipeline(length_ms: int, slide_ms: int, feature_prefix: str):
    login_attempt_schema = LoginAttempt(
        timestamp=datetime.now(),
        user_id="user_21",
        ip_address="127.0.01",
        success=True,
    ).to_dict()

    total_col = f"{feature_prefix}_total"
    success_col = f"{feature_prefix}_success"

    ds = (
        FeastDataStream(
            Context().from_topic(
                TOPIC,
                json.dumps(login_attempt_schema),
                BOOTSTRAP_SERVERS,
            )
        )
        .window(
            [col("user_id")],
            [
                f.count(
                    col("success"), distinct=False, filter=(col("success") == lit(True))
                ).alias(success_col),
                f.count(
                    col("success"),
                    distinct=False,
                    filter=None,
                ).alias(total_col),
            ],
            length_ms,
            slide_ms,
        )
        .with_column(
            f"{feature_prefix}_ratio", col(success_col).cast(float) / col(total_col)
        )
        .with_column("timestamp", col("window_start_time"))
        .drop_columns(["window_start_time", "window_end_time"])
    )

    return ds


def run_pipeline(length_ms: int, slide_ms: int, feature_prefix: str):
    print(
        f"starting pipeline- feature: {feature_prefix}, window_length: {length_ms}, slide_length: {slide_ms}"
    )
    ds = create_pipeline(length_ms, slide_ms, feature_prefix)
    # ds.sink(print_batch)

    repo_path = Path(__file__).parent / "../feature_repo/"
    feature_service = FeatureStore(repo_path=str(repo_path.resolve()))

    def _sink_to_feast(rb: pa.RecordBatch):
        if len(rb):
            df = rb.to_pandas()
            try:
                feature_service.push(f"auth_attempt_push_{feature_prefix}", df, to=PushMode.ONLINE)
                # feature_service.write_to_online_store("auth_attempt_push", df)
            except Exception as e:
                print(e)

    ds.ds.sink_python(_sink_to_feast)

    # ds.print_schema().write_feast_feature(feature_service, f"auth_attempt_push_{feature_prefix}")
