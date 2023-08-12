class FeatureStoreException(Exception):
    """The top-level FeatureStore exception"""


class FeatureDataException(FeatureStoreException):
    """Exception that occurs when something is wrong with the Feature data"""


class MismatchedFeatureException(FeatureDataException):
    """Exception that occurs when trying to merge mismatched Features"""


class FeatureNotFoundException(FeatureStoreException):
    """Exception that occurs when trying to fetch a feature that doesn't exist"""
