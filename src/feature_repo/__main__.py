from pathlib import Path

from feast import FeatureStore
from fraud_data import (auth_attempt, auth_attempt_feature_service,
                        auth_attempt_push_source, auth_attempt_source,
                        auth_attempt_view)

if __name__ == "__main__":
    repo_path = Path(__file__).parent

    fs = FeatureStore(repo_path=str(repo_path.resolve()))
    # fs.teardown()
    fs.apply(
        [
            auth_attempt,
            auth_attempt_feature_service,
            auth_attempt_push_source,
            auth_attempt_source,
            auth_attempt_view,
        ]
    )
