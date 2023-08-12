from feature_store import Client
from feature_store.feature import FeatureKind

client = Client()

neighbourhood_feature = client.register_feature(
    feature_name="neighbourhood",
    kind=FeatureKind.sql,
    location="main.neighbourhood",
    id_column="id",
    date_column="date",
    auth_key="local_sqlite",
)

room_type_feature = client.register_feature(
    feature_name="room_type",
    kind=FeatureKind.sql,
    location="main.room_type",
    id_column="id",
    date_column="date",
    auth_key="local_sqlite",
)

price_feature = client.register_feature(
    feature_name="price",
    kind=FeatureKind.sql,
    location="main.price",
    id_column="id",
    date_column="date",
    auth_key="local_sqlite",
)
