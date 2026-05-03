import snowflake.connector
import pandas as pd
import random
import json
import os
import boto3
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

# =====================================
# CONFIG
# =====================================
SECRET_NAME = os.getenv("SNOWFLAKE_SECRET_NAME", "ga4/snowflake/dbt")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
ROWS_TO_GENERATE = int(os.getenv("ROWS_TO_GENERATE", "40000"))


# =====================================
# LAMBDA ENTRYPOINT
# =====================================
def lambda_handler(event, context):
    try:
        main()
        return {
            "statusCode": 200,
            "body": "Streaming load completed successfully"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }


# =====================================
# MAIN FLOW
# =====================================
def main():

    # ---------------------------------
    # 1. FETCH SECRET
    # ---------------------------------
    secret = get_secret()

    # ---------------------------------
    # 2. BUILD profiles.yml IN /tmp
    # ---------------------------------
    profile_path = "/tmp/profiles.yml"
    build_profiles(secret, profile_path)

    # ---------------------------------
    # 3. READ GENERATED YAML
    # ---------------------------------
    with open(profile_path, "r") as f:
        config = yaml.safe_load(f)

    sf = config["streaming"]["outputs"]["dev"]

    ACCOUNT = sf["account"]
    USER = sf["user"]
    PASSWORD = sf["password"]
    WAREHOUSE = sf["warehouse"]
    DATABASE = sf["database"]
    SCHEMA = sf["schema"]
    ROLE = sf["role"]

    # ---------------------------------
    # 4. FILE + TABLE
    # ---------------------------------
    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    TABLE_NAME = f"EVENTS_{today}"
    FILE_NAME = f"events_{today}.parquet"
    FILE_PATH = f"/tmp/{FILE_NAME}"

    # ---------------------------------
    # 5. CONNECT SNOWFLAKE
    # ---------------------------------
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

    # ---------------------------------
    # 6. GENERATE DATA
    # ---------------------------------
    rows = []

    for i in range(ROWS_TO_GENERATE):

        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ts_micro = ts_ms * 1000

        traffic_source = {
            "source": random.choice(
                ["google", "facebook", "(direct)", "instagram"]
            ),
            "medium": random.choice(
                ["cpc", "organic", "email"]
            ),
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

    # ---------------------------------
    # 7. DATAFRAME -> PARQUET
    # ---------------------------------
    df = pd.DataFrame(rows)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, FILE_PATH)

    # ---------------------------------
    # 8. CREATE TABLE
    # ---------------------------------
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

    # ---------------------------------
    # 9. STAGE
    # ---------------------------------
    cur.execute("""
    CREATE FILE FORMAT IF NOT EXISTS parquet_format
    TYPE = PARQUET
    """)

    cur.execute("""
    CREATE STAGE IF NOT EXISTS demo_stage
    FILE_FORMAT = parquet_format
    """)

    # ---------------------------------
    # 10. PUT FILE
    # ---------------------------------
    cur.execute(f"""
    PUT file://{FILE_PATH}
    @demo_stage
    AUTO_COMPRESS=FALSE
    OVERWRITE=TRUE
    """)

    # ---------------------------------
    # 11. COPY INTO
    # ---------------------------------
    cur.execute(f"""
    COPY INTO {TABLE_NAME}
    FROM @demo_stage/{FILE_NAME}
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FILE_FORMAT = (TYPE = PARQUET)
    """)

    cur.close()
    conn.close()


# =====================================
# GET SECRET
# =====================================
def get_secret():

    client = boto3.client(
        "secretsmanager",
        region_name=AWS_REGION
    )

    response = client.get_secret_value(
        SecretId=SECRET_NAME
    )

    return json.loads(response["SecretString"])


# =====================================
# BUILD profiles.yml
# =====================================
def build_profiles(secret, file_path):

    profile = {
        "streaming": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "snowflake",
                    "account": secret["SNOWFLAKE_ACCOUNT"],
                    "user": secret["SNOWFLAKE_USER"],
                    "password": secret["SNOWFLAKE_PASSWORD"],
                    "role": secret["SNOWFLAKE_ROLE"],
                    "warehouse": secret["SNOWFLAKE_WAREHOUSE"],
                    "database": secret["SNOWFLAKE_DATABASE"],
                    "schema": secret["SNOWFLAKE_SCHEMA"],
                    "threads": 4
                }
            }
        }
    }

    with open(file_path, "w") as f:
        yaml.dump(profile, f)