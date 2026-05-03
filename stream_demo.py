import os
import json
import random
import boto3
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector

from datetime import datetime, timezone

# ======================================================
# ENV VARIABLES
# ======================================================
SECRET_NAME = os.getenv("SNOWFLAKE_SECRET_NAME", "ga4/snowflake/dbt")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
ROWS_TO_GENERATE = int(os.getenv("ROWS_TO_GENERATE", "1000"))

TMP_DIR = "/tmp"
PROFILE_PATH = f"{TMP_DIR}/profiles.yml"


# ======================================================
# LAMBDA ENTRYPOINT
# ======================================================
def lambda_handler(event, context):
    try:
        print("===== Lambda Started =====")

        result = main()

        print("===== Lambda Completed =====")

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        print("===== ERROR =====")
        print(str(e))
        raise e


# ======================================================
# MAIN
# ======================================================
def main():

    # --------------------------------------------------
    # 1. GET SECRET
    # --------------------------------------------------
    print("Fetching Secrets Manager secret...")
    secret = get_secret()

    # --------------------------------------------------
    # 2. BUILD TEMP profiles.yml
    # --------------------------------------------------
    print("Creating /tmp/profiles.yml ...")
    build_profiles(secret, PROFILE_PATH)

    # --------------------------------------------------
    # 3. LOAD CONFIG
    # --------------------------------------------------
    with open(PROFILE_PATH, "r") as f:
        config = yaml.safe_load(f)
    print(config)
    sf = config["streaming"]["outputs"]["dev"]

    # --------------------------------------------------
    # 4. CONNECTION VARIABLES
    # --------------------------------------------------
    ACCOUNT = sf["account"]
    USER = sf["user"]
    PASSWORD = sf["password"]
    WAREHOUSE = sf["warehouse"]
    DATABASE = sf["database"]
    SCHEMA = sf["schema"]
    ROLE = sf["role"]

    # --------------------------------------------------
    # 5. DATE / FILE / TABLE
    # --------------------------------------------------
    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    table_name = f"EVENTS_{today}"
    file_name = f"events_{today}.parquet"
    file_path = f"{TMP_DIR}/{file_name}"

    print(f"Target table: {DATABASE}.{SCHEMA}.{table_name}")

    # --------------------------------------------------
    # 6. CONNECT SNOWFLAKE
    # --------------------------------------------------
    print("Connecting Snowflake...")

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

    try:

        # --------------------------------------------------
        # 7. GENERATE DATA
        # --------------------------------------------------
        print(f"Generating {ROWS_TO_GENERATE} rows...")

        rows = generate_rows(today)

        df = pd.DataFrame(rows)

        # --------------------------------------------------
        # 8. WRITE PARQUET
        # --------------------------------------------------
        print("Writing parquet file...")

        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)

        print("Parquet created:", file_path)

        # --------------------------------------------------
        # 9. CREATE TABLE
        # --------------------------------------------------
        print("Creating Snowflake table if not exists...")

        create_table(cur, table_name)

        # --------------------------------------------------
        # 10. CREATE STAGE
        # --------------------------------------------------
        print("Creating stage / file format...")

        create_stage(cur)

        # --------------------------------------------------
        # 11. PUT FILE
        # --------------------------------------------------
        print("Uploading parquet to Snowflake stage...")

        cur.execute(f"""
            PUT file://{file_path}
            @demo_stage
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE
        """)

        put_result = cur.fetchall()
        print("PUT Result:", put_result)

        # --------------------------------------------------
        # 12. COPY INTO
        # --------------------------------------------------
        print("Loading parquet into Snowflake table...")

        cur.execute(f"""
            COPY INTO {table_name}
            FROM @demo_stage/{file_name}
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FILE_FORMAT = (TYPE = PARQUET)
        """)

        copy_result = cur.fetchall()
        print("COPY Result:", copy_result)

        # --------------------------------------------------
        # 13. VERIFY
        # --------------------------------------------------
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]

        print("Rows loaded:", count)

        return {
            "table_name": table_name,
            "rows_loaded": count
        }

    finally:
        cur.close()
        conn.close()


# ======================================================
# GENERATE DATA
# ======================================================
def generate_rows(today):

    rows = []

    for i in range(ROWS_TO_GENERATE):

        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ts_micro = ts_ms * 1000

        rows.append({

            "event_date": today,
            "event_timestamp": ts_micro,
            "event_name": random.choice(
                ["page_view", "click", "purchase", "session_start"]
            ),

            "event_params": json.dumps([
                {
                    "key": "value",
                    "value": {
                        "double_value": random.choice([10, 20, 50])
                    }
                }
            ]),

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

            "user_properties": json.dumps([]),

            "user_first_touch_timestamp": ts_micro - 1000000,

            "user_ltv": json.dumps({
                "revenue": 100,
                "currency": "USD"
            }),

            "device": json.dumps({
                "category": "mobile"
            }),

            "geo": json.dumps({
                "country": "India"
            }),

            "app_info": json.dumps({
                "id": "demo.app"
            }),

            "traffic_source": json.dumps({
                "source": random.choice(
                    ["google", "facebook", "(direct)"]
                ),
                "medium": "cpc",
                "campaign": "demo_campaign"
            }),

            "stream_id": 1234567890,

            "platform": random.choice(
                ["WEB", "ANDROID", "IOS"]
            ),

            "event_dimensions": json.dumps({
                "hostname": "demo.site.com"
            }),

            "ecommerce": json.dumps({
                "purchase_revenue": 20
            }),

            "items": json.dumps([])

        })

    return rows


# ======================================================
# CREATE TABLE
# ======================================================
def create_table(cur, table_name):

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (

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


# ======================================================
# CREATE STAGE
# ======================================================
def create_stage(cur):

    cur.execute("""
        CREATE FILE FORMAT IF NOT EXISTS parquet_format
        TYPE = PARQUET
    """)

    cur.execute("""
        CREATE STAGE IF NOT EXISTS demo_stage
        FILE_FORMAT = parquet_format
    """)


# ======================================================
# GET SECRET
# ======================================================
def get_secret():

    client = boto3.client(
        "secretsmanager",
        region_name=AWS_REGION
    )

    response = client.get_secret_value(
        SecretId=SECRET_NAME
    )

    return json.loads(response["SecretString"])


# ======================================================
# BUILD profiles.yml
# ======================================================
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