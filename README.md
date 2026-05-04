🌟 main branch README.md

# GA4 Data Platform - Main Branch

Enterprise-grade end-to-end analytics engineering project migrating GA4 data from BigQuery to Snowflake with dbt, AWS, orchestration, CI/CD, streaming ingestion, and dashboards.

<img width="2406" height="1575" alt="ga4_architecture (1)" src="https://github.com/user-attachments/assets/1a2c8ce6-1f46-4d2b-80d9-34d830b7a5ca" />

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
