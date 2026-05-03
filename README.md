🚀 production branch README.md

# Production Branch - dbt Transformations

Production-grade dbt models running on AWS ECS Fargate using Docker images stored in Amazon ECR.

## Responsibilities
- Staging models
- Intermediate transformations
- Fact / dimension marts
- First-click attribution
- Last-click attribution
- Data quality tests

## Deployment Flow
GitHub Push → GitHub Actions → Docker Build → ECR → ECS Task Run

## Core Commands
```bash
 dbt run
 dbt test
 dbt docs generate
