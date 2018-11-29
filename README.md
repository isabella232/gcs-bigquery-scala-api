# BigQuery Scala Api

[ ![Codeship Status for emartech/gcs-bigquery-scala-api](https://app.codeship.com/projects/68e10a10-121a-0135-5667-2a4a553df23d/status?branch=master)](https://app.codeship.com/projects/216915)


## Overview
There are many libraries for Google Cloud Platform except for Scala.

Currently we aim at 
 * Create BigQuery jobs for running queries and tasks
 
## Setup

Usage in sbt:

```"com.emarsys"  %% "gcs-bigquery-scala-api"    % "1.1.0" ```

## Getting Started
 Create api instance:
        
        val bigQueryInstance = BigQueryApi(projectName, Config.credentialWrite)
    
 Create TableReference for a BigQuery table:
 
        val tableReference = BqTableReference("[project]", "[dataSet]", "[table]")
 
 Create command factory which will use the api instance to create commands:
 
         val commandFactory = new CommandFactory {
              override implicit val bigQuery = bigQueryInstance
         }
  
 Following table commands can be used:
 * drop table
 * create table
 * insert data
 
 Following job commands can be used:
  * copy data to table (TableReference)
  * extract data to url in file format (default csv) 
  * insert data from file (default csv)
 
 
 

Testing
------------------

Specify the `GCS_READ_KEY` env var in the `.env` file, then run `sbt test`.


Creating a release
------------------

Bump the version number in `build.sbt` and run the following command:

    sbt publish

This will build a jar with the new version and place it under the `releases` directory.
