⚡ streaming branch README.md

# Streaming Branch - Real-Time Ingestion Pipeline

Serverless streaming simulator generating GA4-like event data and loading Snowflake RAW.EVENTS_YYYYMMDD tables in parquet format.

## Components
- AWS Lambda
- Dockerized Python runtime
- Snowflake connector
- Secrets Manager
- CloudWatch Logs

## Flow
GitHub Push → CI/CD → ECR Image → Lambda Invoke → Generate Events → Parquet → Snowflake RAW

## Features
- Dynamic credentials from AWS Secrets Manager
- Date-partitioned event tables
- Synthetic real-time event generation
- Auto scalable ingestion

## Sample Events
- page_view
- click
- purchase
- session_start
