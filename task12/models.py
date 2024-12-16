from sqlalchemy import Table, Column, Integer, String, MetaData

metadata = MetaData()

clients = Table(
    "clients",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("name", String(255)),
    Column("registration_address", String(255)),
    Column("last_known_location", String(255)),
)
