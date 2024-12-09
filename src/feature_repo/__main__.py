from pathlib import Path

from feast import FeatureStore

from feature_repo.fraud_data import all_entities

if __name__ == "__main__":
    repo_path = Path(__file__).parent

    # Remove sqlite files before re-creating them
    data_dir = repo_path / "data"
    for file_path in data_dir.glob("*"):
        if file_path.is_file():
            file_path.unlink()

    fs = FeatureStore(repo_path=str(repo_path.resolve()))
    try:
        fs.teardown()
    except Exception:
        print("Unable to teardown feature store")

    fs = FeatureStore(repo_path=str(repo_path.resolve()))

    fs.apply(all_entities)
