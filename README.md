# Dataflow Pipeline Examples

This repository contains a collection of Apache Beam Dataflow pipelines, demonstrating various use cases for reading, processing, and writing data using Google Cloud Platform services, such as Pub/Sub, BigQuery, and Cloud Storage.

## Overview

1. **Pub/Sub to BigQuery (Streaming)**: A Dataflow pipeline that reads raw events from a Cloud Pub/Sub subscription and writes the processed data to a BigQuery table.
2. **Pub/Sub to Cloud Storage (Streaming)**: A Dataflow pipeline that reads raw events from a Cloud Pub/Sub topic, processes them, and writes the results to Cloud Storage in windows of a specified size.
3. **Cloud Storage to BigQuery (Batch)**: A Dataflow pipeline that reads files from Cloud Storage, processes them, and writes the results to a BigQuery table in batch mode.
4. **Cloud Storage to BigQuery (Streaming)**: A Dataflow pipeline that reads files from Cloud Storage and writes the processed data to a BigQuery table using streaming inserts.

## Prerequisites

To run the pipelines, you need:

- A Google Cloud Platform project with the necessary APIs enabled
- A Google Cloud Storage bucket for temporary files
- A Google Cloud Pub/Sub topic and subscription
- A Google BigQuery dataset and table

## Setup

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
2. Authenticate with Google Cloud: `gcloud auth login`
3. Set the default project: `gcloud config set project [PROJECT_ID]`
4. Update the necessary variables in the pipeline source code (e.g., project ID, bucket names, topic/subscription names, dataset, and table names)
5. Compile the project: `./gradlew build`

## Running the Pipelines

1. **Pub/Sub to BigQuery (Streaming)**: `./gradlew runPubSubToBigQueryRawEvents -Dexec.args="--inputSub=[SUBSCRIPTION_NAME] --tableBaseName=[BIGQUERY_TABLE_NAME]"`
2. **Pub/Sub to Cloud Storage (Streaming)**: `./gradlew runPubSubToCloudStorageRaw -Dexec.args="--inputTopic=[TOPIC_NAME] --windowSize=[WINDOW_SIZE_IN_MINUTES] --output=[CLOUD_STORAGE_OUTPUT_PATH]"`
3. **Cloud Storage to BigQuery (Batch)**: `./gradlew runCloudStorageToBigQueryBatch -Dexec.args="--inputFilePattern=[CLOUD_STORAGE_FILE_PATTERN] --tableBaseName=[BIGQUERY_TABLE_NAME]"`
4. **Cloud Storage to BigQuery (Streaming)**: `./gradlew runTextIOToBigQueryStreaming -Dexec.args="--inputFileLocation=[CLOUD_STORAGE_INPUT_PATH] --tableBaseName=[BIGQUERY_TABLE_NAME]"`

Replace the placeholders with the appropriate values for your project.

## License

This repository is licensed under the MIT License. See the [LICENSE](https://opensource.org/license/mit/) file for details.
