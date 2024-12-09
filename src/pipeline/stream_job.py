import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from denormalized import Context, FeastDataStream
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit
from feast import FeatureStore
from feast.data_source import PushMode

from session_generator.login_attempt import LoginAttempt


@dataclass
class PipelineConfig:
    window_length_ms: int
    slide_length_ms: int
    feature_prefix: str
    kafka_bootstrap_servers: str
    kafka_topic: str


def run_pipeline(config: PipelineConfig):
    print(
        f"starting pipeline- feature: {config.feature_prefix}, window_length: {config.window_length_ms}, slide_length: {config.slide_length_ms}"
    )

    login_attempt_schema = LoginAttempt(
        timestamp=datetime.now(),
        user_id="user_21",
        ip_address="127.0.01",
        success=True,
    ).to_dict()

    total_col = f"{config.feature_prefix}_total"
    success_col = f"{config.feature_prefix}_success"

    ds = (
        FeastDataStream(
            Context().from_topic(
                config.kafka_topic,
                json.dumps(login_attempt_schema),
                config.kafka_bootstrap_servers,
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
            config.window_length_ms,
            config.slide_length_ms,
        )
        .with_column(
            f"{config.feature_prefix}_ratio",
            col(success_col).cast(float) / col(total_col),
        )
        .with_column("timestamp", col("window_start_time"))
        .drop_columns(["window_start_time", "window_end_time"])
    )

    repo_path = Path(__file__).parent / "../feature_repo/"
    feature_service = FeatureStore(repo_path=str(repo_path.resolve()))

    def _sink_to_feast(rb: pa.RecordBatch):
        if len(rb):
            df = rb.to_pandas()
            try:
                feature_service.push(
                    f"auth_attempt_push_{config.feature_prefix}", df, to=PushMode.ONLINE
                )
            except Exception as e:
                print(e)

    ds.ds.sink_python(_sink_to_feast)

    # ds.print_schema().write_feast_feature(feature_service, f"auth_attempt_push_{feature_prefix}")
