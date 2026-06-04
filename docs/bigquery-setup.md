# BigQuery Ingestion Setup

Follow these steps to make sure every `/p.gif` event is pushed to BigQuery while the pixel response stays instant.

## 1. Prepare BigQuery

1. Create (or reuse) a dataset via the BigQuery console:
   1. Open https://console.cloud.google.com/bigquery and select the correct GCP project.
   2. In the left sidebar, click the three-dot menu next to the project and choose **Create dataset**.
   3. Enter `playable_tracking` (or your preferred name), select a region, leave encryption as Google-managed, and press **Create dataset**.
2. Create 2 partitioned tables for the events using the SQL workspace:
   1. Click **Compose new query** in the console.
   2. Paste the schema below and hit **Run**:

   ```sql
   CREATE TABLE `playable_tracking.pixel_events_ver_2`
   (
     session_id   STRING    NOT NULL,
     event_name   STRING    NOT NULL,
     event_time   TIMESTAMP NOT NULL,
     package_name STRING,
     playable_id  STRING,
     ip           STRING,
     referer      STRING,
     received_at  TIMESTAMP NOT NULL,
     event_params STRING,
     event_hash   STRING
   )
   PARTITION BY DATE(received_at);

   CREATE TABLE `playable_tracking.pixel_events_production`
   (
     session_id   STRING    NOT NULL,
     event_name   STRING    NOT NULL,
     event_time   TIMESTAMP NOT NULL,
     package_name STRING,
     playable_id  STRING,
     ip           STRING,
     referer      STRING,
     received_at  TIMESTAMP NOT NULL,
     event_params STRING,
     event_hash   STRING
   )
   PARTITION BY DATE(received_at);
   ```

   Partitioning on `received_at` keeps storage queries efficient as traffic grows.
   `event_params` is stored as a JSON string. `event_time` is the client-reported timestamp;
   `received_at` is the server arrival time stamped automatically — never trust `event_time` alone for ordering.

   **Existing deployments (zero migration):** Old-schema tables already contain `package_name`,
   `playable_id`, `ip`, `referer` (the server writes all of these) plus three legacy columns the
   server no longer writes (`platform`, `campaign_raw`, `user_agent`) — new rows leave those three `NULL`.
   No `ALTER TABLE` is required to keep the server running. See §6 for optional cleanup DDL to drop the three.

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
```

`BIGQUERY_ENABLED` gates the insert logic; if it is `false` (default) the server will skip BigQuery writes while still logging to `logs/pixel-tracking.txt`.
The `env` query param decides the destination table:
- `env=production` → `playable_tracking.pixel_events_production`
- `env=test` (or missing/unknown value) → `playable_tracking.pixel_events_ver_2`

## 3. Deploy

1. Run `scripts/deploy-prod.sh`.
2. The script verifies `@google-cloud/bigquery` is installed, checks the env vars when `BIGQUERY_ENABLED=true`, and injects them into the container with `-e BIGQUERY_*`.
3. When the script finishes it prints the health and pixel URLs. Hit them to confirm the service is live.

## 4. Client API

Every event is a `GET /p.gif` request with these query parameters:

| Param | Required | Description |
|---|---|---|
| `e` | yes | Event name: `start`, `interaction`, `store_trigger`, or `end` |
| `sid` | yes | Session ID — a stable UUID for the playable session |
| `event_time` | yes | Client-side event time as an ISO 8601 string (e.g. `2026-06-04T07:00:00.000Z`) |
| `event_params` | yes | URL-encoded JSON object; shape depends on event type (see below) |
| `env` | no | `production` routes to the production table; anything else uses the test table |

`received_at` is **always server-generated** — clients must not send it.

### Per-event `event_params` shapes

**`start`** — sent once when the playable loads:
```json
{
  "platform": "android",
  "campaign": {
    "network": "meta",
    "campaign_id": "camp_001",
    "campaign_name": "summer_2026",
    "adgroup_id": "ag_1",
    "creative_id": "creative_3",
    "click_id": "click_abc",
    "country": "VN"
  }
}
```

**`interaction`** — sent on each meaningful user interaction:
```json
{ "name": "tap" }
```

**`store_trigger`** — sent when the user taps the install / store CTA:
```json
{ "name": "tap_cta" }
```

**`end`** — sent when the playable session ends (user exits, ad closes, timeout):
```json
{ "interact_count": 4 }
```

## 5. Verify ingestion

Send one sample request per event type (test table):

```bash
SERVER="http://<SERVER_IP>:9000"
SID="test-session-$(date +%s)"

# start
curl "$SERVER/p.gif?e=start&sid=$SID&event_time=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)&env=test&event_params=%7B%22platform%22%3A%22ios%22%2C%22campaign%22%3A%7B%22network%22%3A%22meta%22%2C%22campaign_id%22%3A%22c1%22%7D%7D"

# interaction
curl "$SERVER/p.gif?e=interaction&sid=$SID&event_time=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)&env=test&event_params=%7B%22name%22%3A%22tap%22%7D"

# store_trigger
curl "$SERVER/p.gif?e=store_trigger&sid=$SID&event_time=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)&env=test&event_params=%7B%22name%22%3A%22tap_cta%22%7D"

# end
curl "$SERVER/p.gif?e=end&sid=$SID&event_time=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)&env=test&event_params=%7B%22interact_count%22%3A1%7D"
```

Tail `logs/pixel-tracking.txt`; you should see a JSON entry for each request.

Query BigQuery to confirm ingestion:

```sql
SELECT event_name, session_id, event_time, received_at,
       JSON_VALUE(event_params, '$.platform') AS platform
FROM `playable_tracking.pixel_events_ver_2`
WHERE session_id = 'test-session-<YOUR_SID>'
ORDER BY event_time ASC;
```

`event_hash` lets you dedupe retried inserts if needed.

## 6. Optional cleanup DDL

The server no longer writes three legacy columns (`platform`, `campaign_raw`, `user_agent`).
They remain in existing tables with `NULL` on all new rows. The server works correctly
whether or not these columns are present — this cleanup is purely cosmetic.
(Do **not** drop `package_name`, `playable_id`, `ip`, or `referer` — the server still writes those.)

**Before running:** BigQuery requires a column to have no streaming-buffer or time-travel data before it
can be dropped. Wait at least 7 days after the last write to any of these columns (i.e., after fully
replacing the old server version) and back up any historical values you want to keep.

```sql
-- pixel_events_ver_2: drop legacy columns (optional)
ALTER TABLE `playable_tracking.pixel_events_ver_2`
  DROP COLUMN IF EXISTS platform,
  DROP COLUMN IF EXISTS campaign_raw,
  DROP COLUMN IF EXISTS user_agent;

-- pixel_events_production: same cleanup (optional)
ALTER TABLE `playable_tracking.pixel_events_production`
  DROP COLUMN IF EXISTS platform,
  DROP COLUMN IF EXISTS campaign_raw,
  DROP COLUMN IF EXISTS user_agent;
```

With these steps complete every incoming event will be persisted both in the log file and in BigQuery.
