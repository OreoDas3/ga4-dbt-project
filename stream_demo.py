import snowflake.connector
import pandas as pd
import random
import json
import os
from datetime import datetime, timezone
import yaml
import pyarrow as pa
import pyarrow.parquet as pq

# =====================================
# LOAD CONFIG
# =====================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "profiles.yml")

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

sf = config["streaming"]["outputs"]["dev"]

ACCOUNT = sf["account"]
USER = sf["user"]
PASSWORD = sf["password"]
WAREHOUSE = sf["warehouse"]
DATABASE = sf["database"]
SCHEMA = sf["schema"]
ROLE = sf["role"]

ROWS_TO_GENERATE = sf.get("rows_to_generate", 40000)

# =====================================
# DATE + FILE
# =====================================
today = datetime.now(timezone.utc).strftime("%Y%m%d")

TABLE_NAME = f"EVENTS_{today}"
FILE_NAME = f"events_{today}.parquet"
FILE_PATH = os.path.join(BASE_DIR, FILE_NAME)

# =====================================
# CONNECT SNOWFLAKE
# =====================================
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    role=ROLE
)

cur = conn.cursor()

# =====================================
# GENERATE DATA
# =====================================
rows = []

for i in range(ROWS_TO_GENERATE):

    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    ts_micro = ts_ms * 1000

    traffic_source = {
        "source": random.choice(["google", "facebook", "(direct)", "instagram"]),
        "medium": random.choice(["cpc", "organic", "email"]),
        "campaign": "demo_campaign"
    }

    event_params = [
        {
            "key": "value",
            "value": {
                "double_value": random.choice([10, 20, 50])
            }
        },
        {
            "key": "page_location",
            "value": {
                "string_value": random.choice(
                    ["/home", "/product", "/checkout"]
                )
            }
        }
    ]

    rows.append({

        "event_date": today,
        "event_timestamp": ts_micro,
        "event_name": random.choice(
            ["page_view", "click", "purchase", "session_start"]
        ),
        "event_params": json.dumps(event_params),
        "event_previous_timestamp": ts_micro - 5000,
        "event_value_in_usd": float(random.choice([10, 20, 50])),
        "event_bundle_sequence_id": i + 1,
        "event_server_timestamp_offset": 0,

        "user_id": None,
        "user_pseudo_id": f"user_{random.randint(1,50)}",

        "privacy_info": json.dumps({
            "analytics_storage": "Yes",
            "ads_storage": "Yes"
        }),

        "user_properties": json.dumps([
            {
                "key": "customer_type",
                "value": {
                    "string_value": random.choice(
                        ["new", "returning"]
                    )
                }
            }
        ]),

        "user_first_touch_timestamp": ts_micro - 1000000,

        "user_ltv": json.dumps({
            "revenue": random.choice([100, 250, 500]),
            "currency": "USD"
        }),

        "device": json.dumps({
            "category": "mobile",
            "operating_system": random.choice(
                ["Android", "iOS"]
            ),
            "language": "en"
        }),

        "geo": json.dumps({
            "country": "India",
            "city": random.choice(
                ["Kolkata", "Delhi", "Mumbai"]
            )
        }),

        "app_info": json.dumps({
            "id": "com.demo.app",
            "version": "1.0.0"
        }),

        "traffic_source": json.dumps(traffic_source),

        "stream_id": 1234567890,

        "platform": random.choice(
            ["WEB", "ANDROID", "IOS"]
        ),

        "event_dimensions": json.dumps({
            "hostname": "demo.site.com"
        }),

        "ecommerce": json.dumps({
            "purchase_revenue": random.choice([10, 20, 50]),
            "transaction_id": f"txn_{i+1}"
        }),

        "items": json.dumps([
            {
                "item_id": f"SKU_{random.randint(1,20)}",
                "item_name": "Demo Product",
                "price": random.choice([10, 20, 50]),
                "quantity": 1
            }
        ])

    })

# =====================================
# DATAFRAME
# =====================================
df = pd.DataFrame(rows)

# =====================================
# WRITE PARQUET
# =====================================
table = pa.Table.from_pandas(df)
pq.write_table(table, FILE_PATH)

print(f"Created file: {FILE_NAME}")

# =====================================
# CREATE TABLE EXACT STRUCTURE
# =====================================
cur.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (

event_date VARCHAR,
event_timestamp NUMBER(38,0),
event_name VARCHAR,
event_params VARIANT,
event_previous_timestamp NUMBER(38,0),
event_value_in_usd FLOAT,
event_bundle_sequence_id NUMBER(38,0),
event_server_timestamp_offset NUMBER(38,0),

user_id VARCHAR,
user_pseudo_id VARCHAR,

privacy_info VARIANT,
user_properties VARIANT,
user_first_touch_timestamp NUMBER(38,0),
user_ltv VARIANT,

device VARIANT,
geo VARIANT,
app_info VARIANT,
traffic_source VARIANT,

stream_id NUMBER(38,0),
platform VARCHAR,

event_dimensions VARIANT,
ecommerce VARIANT,
items VARIANT

)
""")

# =====================================
# FILE FORMAT + STAGE
# =====================================
cur.execute("""
CREATE FILE FORMAT IF NOT EXISTS parquet_format
TYPE = PARQUET
""")

cur.execute("""
CREATE STAGE IF NOT EXISTS demo_stage
FILE_FORMAT = parquet_format
""")

# =====================================
# UPLOAD FILE
# =====================================
cur.execute(f"""
PUT file://{FILE_PATH}
@demo_stage
AUTO_COMPRESS=FALSE
OVERWRITE=TRUE
""")

# =====================================
# COPY INTO
# =====================================
cur.execute(f"""
COPY INTO {TABLE_NAME}
FROM @demo_stage/{FILE_NAME}
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET)
""")

print(f"Loaded data into RAW.{TABLE_NAME}")

cur.close()
conn.close()