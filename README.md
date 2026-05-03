🌟 main branch README.md

# GA4 Data Platform - Main Branch

Enterprise-grade end-to-end analytics engineering project migrating GA4 data from BigQuery to Snowflake with dbt, AWS, orchestration, CI/CD, streaming ingestion, and dashboards.

## Tech Stack
- Snowflake
- dbt Core
- AWS Lambda
- ECS Fargate
- ECR
- Step Functions
- SNS Alerts
- GitHub Actions
- Streamlit / Snowflake Native Streamlit

## Architecture
BigQuery → Snowflake RAW → dbt Models → Analytics Layer → Dashboards

## Branches
- production → dbt production pipelines
- streaming → Lambda real-time ingestion
- dashboards → Streamlit dashboards

## Highlights
- Automated CI/CD
- Containerized workloads
- Orchestrated workflows
- Real-time event simulation
- Attribution analytics
