class FeatureStoreException(Exception):
    """The top-level FeatureStore exception"""


class FeatureDataException(FeatureStoreException):
    """Exception that occurs when something is wrong with the Feature data"""
