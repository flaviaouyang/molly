from molly.features.completeness import Completeness
from molly.features.Staleness import Staleness


def feature_factory(feature_name: str, **kwargs):
    if feature_name == "completeness":
        return Completeness(**kwargs)
    elif feature_name == "staleness":
        return Staleness(**kwargs)
    else:
        raise ValueError(f"Feature {feature_name} is not supported.")
