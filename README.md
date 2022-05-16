# DataEngineering: data_modelling_aws 
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their dataset comprises user logs and music records resides in S3, in a directory of JSON user logs (user activity), as well as a directory with JSON song metadata. This repo is builds an ETL pipeline that extracts the data from S3, stages it in Redshift, and transforms data into a set of SQL fact and dimension tables (basically an unstructured to structured transformation).

## Database design:
The database is split into two schemas, staging and final.

### Staging schema
The staging schema loads events data into one event staging table, and song data into one song table.

### Staging tables:
#### events - colname, datatype, constraint:
* artist VARCHAR
* auth VARCHAR
* firstName VARCHAR
* gender VARCHAR
* itemInSession INT
* lastName VARCHAR
* length REAL
* level VARCHAR
* location VARCHAR
* method VARCHAR
* page VARCHAR
* registration REAL
* sessionId INT
* song VARCHAR
* status INT
* ts BIGINT
* userAgent VARCHAR
* userId INT
* songs - colname, datatype, constraint:
* num_songs INT
* artist_id VARCHAR
* artist_latitude REAL
* artist_longitude REAL
* artist_location VARCHAR
* artist_name VARCHAR
* song_id VARCHAR
* title VARCHAR
* duration REAL
* year INT

### Final schema.
The final schema follows the star schema principle. The tables are listed below:

### Fact tables:
#### songplays - colname, datatype, constraint:
* songplay_id INT PRIMARY KEY
* start_time BIGINT
* user_id VARCHAR
* level VARCHAR
* song_id VARCHAR
* artist_id VARCHAR
* session_id INT
* location VARCHAR
* user_agent VARCHAR
### Dimension tables:
#### users - colname, datatype, constraint:
* user_id INT PRIMARY KEY
* first_name VARCHAR
* last_name VARCHAR
* gender VARCHAR
* level VARCHAR
#### songs - colname, datatype, constraint:
* song_id VARCHAR PRIMARY KEY
* title VARCHAR
* artist_id VARCHAR
* year INT
* duration REAL

#### artists - colname, datatype, constraint:
* artist_id VARCHAR
* name VARCHAR
* location VARCHAR
* latitude REAL
* longitude REAL
#### time - colname, datatype, constraint:
* start_time TIMESTAMP WITH TIME ZONE
* hour INT
* day INT
* week INT
* month INT
* year INT
* weekday VARCHAR
## Code:
### Functional content
* create_tables.py - connects to redshift, drops and creates schemas, dbs and tables
* etl.py - copies json data from s3 buckets into redshift staging, transforms and loads into the final (star) schema. DUPLICATE ENTRIES are handled with ON CONFLICT DO NOTHING.
* sql_queries - all sql code resides here and is called from the above two modules.

## How to execute :
* create an AWS Redshift cluster, noting the following details: [endpoint, db name, db user, db password, db port]
* open dwh.cfg and overwrite CLUSTER>HOST, DB_NAME, DB_USER, DB_PASSWORD, and DB_PORT, with the relevant details.
* In a terminal in /home/workspace, run python etl.py
