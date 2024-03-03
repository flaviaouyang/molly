from molly.features.completeness import Completeness


def feature_factory(feature_name: str, **kwargs):
    if feature_name == "completeness":
        return Completeness(**kwargs)
    else:
        raise ValueError(f"Feature {feature_name} is not supported.")
