# BigQuery Scala Api

[ ![Codeship Status for emartech/gcs-bigquery-scala-api](https://app.codeship.com/projects/68e10a10-121a-0135-5667-2a4a553df23d/status?branch=master)](https://app.codeship.com/projects/216915)


## Overview
There are many library for Google Cloud Platform except Scala. This library for Scala user who would like to write api code in Scala.


Currently we aim at 
 * Create BigQuery jobs for running queries and tasks
 

## Setup

Usage in sbt:

```"com.emarsys"  %% "gcs-bigquery-scala-api"    % "1.0.8" ```

## Getting Started
 Create api instance:
        
        val bigQueryInstance = BigQueryApi(projectName, Config.credentialWrite)

Testing
------------------

Creating a release
------------------

Bump the version number in `build.sbt` and run the following command:

    sbt publish

This will build a jar with the new version and place it under the `releases` directory.
