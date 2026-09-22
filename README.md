# BigQuery to Remotely Store Eye API Sync

This service reads new violation rows from a BigQuery table and pushes them to the Remotely Store Eye API.

## How it works
1. Queries BigQuery for rows newer than the last processed timestamp.
2. Downloads the snapshot image from Google Cloud Storage.
3. Uploads the image to the `/v2/webhooks/files` endpoint.
4. Creates a violation via the `/v1/webhooks/violations` endpoint.

## Setup
1. Run `npm install`
2. Create a `.env` file with the following:
3. Run locally: `npm start`

## Deployment
Deployed via Google Cloud Run. Cloud Scheduler triggers the endpoint every 5 minutes.