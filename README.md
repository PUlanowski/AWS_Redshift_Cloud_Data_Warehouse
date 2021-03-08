#Cloud Data Warehouse for Sparkify

##This project is to provide new cloud architecture for data analytics.

###Background:
Curently data about usage of Sparkify is gathered in form of .json files and stored in S3 storage. Team requires data infrastructure that allow accurate and efficient query processing.

###Solution:
Idea of implementation would be to build ETL pipline that will run logic as follow:
1. Drop all old tables if any exist on Redshift cluster
2. Create staging tables and building new star schema
3. Fetch and process data from .json files on S3 storage and insert into staging tables
4. Using staging tables as source to parse into star schema tables 

###Prerequisites:
1. Access to S3 storage need to be provided or public accessible
2. Redshift cluster of appropriate parameters need to be created beforehand
3. dwh.cfg need to be updated with correct credentials and connection parameters

###Star schema:

####Fact table : Songplays
This table contains all records describing users activities regarding song selection on Sparkify portal.
####Dimension tables:
Users - users description
Songs - songs selected by users
Artists - artists data associated with songs
Time - timestamp of records broken down to readable format

###SQL examples to run in Redshift query editor
1. To check if tables are created:\
SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public';
