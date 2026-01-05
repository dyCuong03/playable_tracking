# BigQuery Ingestion Setup

Follow these steps to make sure every `/p.gif` event is pushed to BigQuery while the pixel response stays instant.

## 1. Prepare BigQuery

1. Create (or reuse) a dataset via the BigQuery console:
   1. Open https://console.cloud.google.com/bigquery and select the correct GCP project.
   2. In the left sidebar, click the three-dot menu next to the project and choose **Create dataset**.
   3. Enter `playable_tracking` (or your preferred name), select a region, leave encryption as Google-managed, and press **Create dataset**.
2. Create a partitioned table for the events using the SQL workspace:
   1. Click **Compose new query** in the console.
   2. Paste the schema below and hit **Run** (adjust dataset/table names if needed):

   ```sql
   CREATE TABLE `playable_tracking.pixel_events`
   (
     event_time TIMESTAMP,
     event_name STRING,
     project_id STRING,
     playable_id STRING,
     session_id STRING,
     event_params STRING,
     ip STRING,
     user_agent STRING,
     referer STRING,
     received_at TIMESTAMP,
     event_hash STRING
   )
   PARTITION BY DATE(received_at);
   ```

   Partitioning on `received_at` keeps storage queries efficient as traffic grows.

3. Provision a service account with permission to insert rows through the Cloud Console:
   1. Go to https://console.cloud.google.com/iam-admin/serviceaccounts.
   2. Click **Create service account**, give it a descriptive name (e.g. `pixel-tracker-writer`), and continue.
   3. Grant it the **BigQuery Data Editor** role (or a role with table insert permissions) for your project/dataset.
   4. Open the new service account, switch to the **Keys** tab, choose **Add key → Create new key → JSON**, and download the file. Copy it to the server, e.g. `/etc/pixel-sa.json`.

## 2. Configure the server

Set the Google client credential path and BigQuery flags for the process that runs `node src/server.js` or `scripts/deploy-prod.sh`:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/etc/pixel-sa.json
export BIGQUERY_ENABLED=true
export BIGQUERY_DATASET=playable_tracking
export BIGQUERY_TABLE=pixel_events
```

`BIGQUERY_ENABLED` gates the insert logic; if it is `false` (default) the server will skip BigQuery writes while still logging to `logs/pixel-tracking.txt`.

## 3. Deploy

1. Run `scripts/deploy-prod.sh`.
2. The script verifies `@google-cloud/bigquery` is installed, checks the env vars when `BIGQUERY_ENABLED=true`, and injects them into the container with `-e BIGQUERY_*`.
3. When the script finishes it prints the health and pixel URLs. Hit them to confirm the service is live.

## 4. Verify ingestion

1. Send a sample pixel request, e.g.
   ```
   curl "http://<SERVER_IP>:9000/p.gif?e=test&pid=my-project&playableId=demo1&sid=abc&ts=1736179200000&campaign=summer"
   ```
2. Tail `logs/pixel-tracking.txt`; you should see a JSON entry containing all fields plus `delay_time`.
3. Query the BigQuery table:
   ```sql
   SELECT event_name, project_id, event_params
   FROM `playable_tracking.pixel_events`
   WHERE event_name = 'test'
   ORDER BY received_at DESC
   LIMIT 10;
   ```
4. Confirm the row shows up; `event_hash` lets you dedupe if needed.

With these steps complete every incoming event will be persisted both in the log file and in BigQuery.***
