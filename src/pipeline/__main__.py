import json
import pprint as pp
from pathlib import Path

from feast import FeatureStore

from stream_job import ds


def print_batch(rb):
    if len(rb):
        print(rb.to_pydict())


if __name__ == "__main__":
    ds.sink(print_batch)

    # repo_path = Path(__file__).parent / "../feature_repo/"
    #
    # feature_service = FeatureStore(repo_path=str(repo_path.resolve()))
    # ds.write_feast_feature(feature_service, "push_sensor_statistics")
