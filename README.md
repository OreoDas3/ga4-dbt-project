🚀 production branch

# Production Branch - dbt Transformations

Production-grade dbt models running on AWS ECS Fargate using Docker images stored in Amazon ECR.
<img width="2406" height="1575" alt="ga4_architecture (1)" src="https://github.com/user-attachments/assets/1a2c8ce6-1f46-4d2b-80d9-34d830b7a5ca" />
## Responsibilities
- Staging models
- Intermediate transformations
- Fact / dimension marts
- First-click attribution
- Last-click attribution
- Data quality tests

## Deployment Flow
GitHub Push → GitHub Actions → Docker Build → ECR → ECS Task Run

## Infra
- ECS Cluster
- Task Definitions
- Secrets Manager
- CloudWatch Logs

## Core Commands
```bash
 dbt run
 dbt test
 dbt docs generate




 

