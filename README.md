# AWS Data Restore

# Demo Video

[![AWS Data Restore Demo Video](https://img.youtube.com/vi/xbYQuewYPh4/0.jpg)](https://www.youtube.com/watch?v=xbYQuewYPh4)

# Architectural Diagram

![alt text](https://github.com/kyle-guanyi/sparc2023/blob/main/public/Data%20Restore%20Architectural%20Diagram.png?raw=true)

This repository holds some of the tools we used in our architectural diagram above:
- Python script that populates our MySQL RDS database via TMDB API (import_to_RDS.py)
- Python AWS Lambda functions that randomly edit our database (aws-random-edit-sparc-db) and exports completed snapshots to an S3 bucket (aws-rds-s3-v2).
- Python script that can export any snapshot in any of our S3 buckets to AWS DynamoDB (s3_to_dynamoDB.py)

