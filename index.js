if (process.env.NODE_ENV !== "production") {
  require("dotenv").config();
}

require("events").EventEmitter.defaultMaxListeners = 20;

const { BigQuery } = require("@google-cloud/bigquery");
const { Storage } = require("@google-cloud/storage");
const { Firestore } = require("@google-cloud/firestore");
const axios = require("axios");
const FormData = require("form-data");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const customParseFormat = require("dayjs/plugin/customParseFormat");

dayjs.extend(utc);
dayjs.extend(customParseFormat);

// ─── GCP Clients ─────────────────────────────────────────────────────
const bigquery = new BigQuery();
const storage = new Storage();
const firestore = new Firestore();

// ─── Shared Config ───────────────────────────────────────────────────
const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const CLIENT_KEY = process.env.CLIENT_KEY;
const EDGE_ID = process.env.EDGE_ID;
const BASE_URL = process.env.BASE_URL;
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL || "";

const MAX_ATTEMPTS = 10;
const WARN_AFTER_ATTEMPTS = 5;
const PERMANENT_HTTP_STATUSES = [400, 401, 403, 404, 405, 410, 422];

// ═══════════════════════════════════════════════════════════════════════
// DATASET CONFIG MAP — one entry per table
// ═══════════════════════════════════════════════════════════════════════
const DATASET_CONFIG = {
  ppe: {
    source: "bigquery",
    tableId: "namaq_ppe",
    stateDocPath: "pipeline_state/ppe_job",
    auditCollection: "sync_audit_ppe",
    timeField: "violation_time",
    typeValue: "PPE",
    hasImage: true,
    selectFields: [
      "id",
      "branch_id",
      "camera_id",
      "ppe_person_id",
      "violation_type",
      "violation_time",
      "confidence",
      "snapshot",
      "snapshot_url",
    ],
    extraFields: [
      "ppe_person_id",
      "violation_type",
      "confidence",
      "snapshot",
      { from: "camera_id", to: "zone_id" },
      "branch_id",
    ],
  },
  footfall: {
    source: "bigquery",
    tableId: "namaq_footfall",
    stateDocPath: "pipeline_state/footfall_job",
    auditCollection: "sync_audit_footfall",
    timeField: "event_time",
    typeValue: "FOOTFALL",
    hasImage: false,
    selectFields: [
      "id",
      "branch_id",
      "camera_id",
      "track_id",
      "direction",
      "event_time",
    ],
    extraFields: [
      "track_id",
      "direction",
      { from: "camera_id", to: "zone_id" },
      "branch_id",
    ],
  },
  heatmap: {
    source: "bigquery",
    tableId: "namaq_heatmap",
    stateDocPath: "pipeline_state/heatmap_job",
    auditCollection: "sync_audit_heatmap",
    timeField: "event_time",
    typeValue: "HEATMAP",
    hasImage: false,
    selectFields: [
      "id",
      "branch_id",
      "camera_id",
      "x",
      "y",
      "track_id",
      "event_time",
    ],
    // camera_id is renamed to zone_id in the extra payload for heatmap
    extraFields: [
      "x",
      "y",
      "track_id",
      { from: "camera_id", to: "zone_id" },
      "branch_id",
    ],
  },
  staff_occupancy: {
    source: "bigquery",
    tableId: "namaq_staff_occupancy",
    stateDocPath: "pipeline_state/staff_occupancy_job",
    auditCollection: "sync_audit_staff_occupancy",
    timeField: "event_time",
    typeValue: "STAFF_OCCUPANCY",
    hasImage: false,
    selectFields: ["id", "branch_id", "camera_id", "staff_count", "event_time"],
    extraFields: [
      "staff_count",
      { from: "camera_id", to: "zone_id" },
      "branch_id",
    ],
  },
    table_cleaning: {
    source: "bigquery",
    tableId: "namaq_table_cleaning",
    stateDocPath: "pipeline_state/table_cleaning_job",
    auditCollection: "sync_audit_table_cleaning",
    timeField: "start_time",
    typeValue: "TABLE_CLEANING",
    hasImage: true,
    selectFields: [
      "id",
      "branch_id",
      "camera_id",
      "table_id",
      "start_time",
      "end_time",
      "duration",
      "snapshot",
      "snapshot_url",
    ],
    extraFields: [
      "table_id",
      "duration",
      "start_time",
      "end_time",
      "snapshot",
      { from: "camera_id", to: "zone_id" },
      "branch_id",
    ],
  },
  heatmap_frames: {
    source: "gcs",
    stateDocPath: "pipeline_state/heatmap_frames_job",
    auditCollection: "sync_audit_heatmap_frames",
    typeValue: "HEATMAP_FRAME",
    branchId: "4381",
    frames: [
      {
        zoneId: "03",
        url: "https://storage.googleapis.com/si-heatmap-frames/namqcafe_heatmap_frames/03.jpg",
      },
      {
        zoneId: "08",
        url: "https://storage.googleapis.com/si-heatmap-frames/namqcafe_heatmap_frames/08.jpg",
      },
    ],
  },
};

// ═══════════════════════════════════════════════════════════════════════
// ENTRY POINT
// ═══════════════════════════════════════════════════════════════════════
exports.processNewRows = async (req, res) => {
  try {
    const mode = ((req.query && req.query.mode) || "ppe").toLowerCase();
    const config = DATASET_CONFIG[mode];

    if (!config) {
      return res
        .status(400)
        .send(`Invalid mode. Use: ${Object.keys(DATASET_CONFIG).join(" | ")}`);
    }

    // GCS source (daily frame uploads) — bypasses the BigQuery flow entirely
    if (config.source === "gcs") {
      return await processGcsFrames(mode, config, res);
    }

    console.log(
      `🚀 Starting ${mode.toUpperCase()} sync from ${config.tableId}...`,
    );

    const watermark = await getWatermark(config.stateDocPath);
    console.log(`📅 Watermark: ${watermark}`);

    const selectList = config.selectFields.join(", ");
    const [rows] = await bigquery.query({
      query: `
        SELECT ${selectList}
        FROM \`${PROJECT_ID}.${DATASET_ID}.${config.tableId}\`
        WHERE ${config.timeField} > @watermark
        ORDER BY ${config.timeField} ASC
        LIMIT 50
      `,
      params: { watermark: new Date(watermark) },
    });

    console.log(`📊 Found ${rows.length} rows.`);
    if (rows.length === 0) return res.status(200).send("No new rows.");

    let succeeded = 0,
      abandoned = 0,
      pendingRetry = 0;

    for (const row of rows) {
      const result = await processRow(row, config);
      const isoTime = toIsoTimestamp(row[config.timeField]);

      if (result === "SUCCESS" || result === "ABANDONED") {
        await setWatermark(config.stateDocPath, isoTime);
        if (result === "SUCCESS") succeeded++;
        else abandoned++;
      } else if (result === "RETRY_PENDING") {
        pendingRetry++;
        console.log(`  ⏸ Row ${row.id} pending retry. Stopping batch.`);
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    return res
      .status(200)
      .send(
        `✅ ${mode.toUpperCase()} Processed: ${succeeded} ok, ${abandoned} abandoned, ${pendingRetry} pending-retry.`,
      );
  } catch (err) {
    console.error("❌ Fatal error:", err);
    return res.status(500).send("Internal Server Error");
  }
};

// ═══════════════════════════════════════════════════════════════════════
// GCS FRAME PROCESSING (daily reference image upload)
// ═══════════════════════════════════════════════════════════════════════
async function processGcsFrames(mode, config, res) {
  const todayUtc = dayjs.utc().format("YYYY-MM-DD");

  // Check Firestore: already uploaded today?
  const stateRef = firestore.doc(config.stateDocPath);
  const stateDoc = await stateRef.get();
  const lastDate = stateDoc.exists ? stateDoc.data().lastFrameDate : null;

  if (lastDate === todayUtc) {
    console.log(`⏭ Frames already uploaded today (${todayUtc}). Skipping.`);
    return res.status(200).send(`Frames already uploaded for ${todayUtc}.`);
  }

  console.log(`🚀 Starting ${mode.toUpperCase()} upload for ${todayUtc}...`);

  let succeeded = 0,
    abandoned = 0;

  for (const frame of config.frames) {
    const docId = `frame_${frame.zoneId}_${todayUtc}`;
    console.log(`🔹 Frame ${frame.zoneId} — ${frame.url}`);

    try {
      const fileId = await uploadFile(frame.url);
      console.log(`  ✓ Uploaded: ${fileId}`);

      await createViolation({
        cameraId: EDGE_ID,
        type: config.typeValue,
        detectedAt: new Date().toISOString(),
        fileId: fileId,
        extra: [
          {
            id: docId,
            zone_id: frame.zoneId,
            branch_id: config.branchId,
          },
        ],
      });
      console.log(`  ✓ ${config.typeValue} frame created.`);

      await writeAudit({ id: docId }, config, {
        status: "SUCCESS",
        attemptCount: 1,
        resolvedAt: new Date().toISOString(),
        error: null,
        zoneId: frame.zoneId,
      });
      succeeded++;
    } catch (err) {
      console.error(`  ⛔ Frame ${frame.zoneId} failed: ${err.message}`);
      await writeAudit({ id: docId }, config, {
        status: "ABANDONED",
        attemptCount: 1,
        abandonedAt: new Date().toISOString(),
        error: sanitizeError(err),
        abandonReason: "permanent_error",
        zoneId: frame.zoneId,
      });
      abandoned++;
    }

    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  // Mark today as complete only if all frames succeeded
  if (abandoned === 0) {
    await stateRef.set(
      { lastFrameDate: todayUtc, updatedAt: new Date().toISOString() },
      { merge: true },
    );
    console.log(`💾 Marked ${todayUtc} as complete.`);
  } else {
    console.log(`⚠️ Some frames failed — will retry next run.`);
  }

  return res
    .status(200)
    .send(
      `✅ ${mode.toUpperCase()} Processed: ${succeeded} ok, ${abandoned} abandoned.`,
    );
}

// ═══════════════════════════════════════════════════════════════════════
// ROW PROCESSING
// ═══════════════════════════════════════════════════════════════════════
async function processRow(row, config) {
  console.log(`🔹 Row ${row.id}`);

  const existingAudit = await getAuditDoc(config.auditCollection, row.id);
  const previousAttempts = existingAudit?.attemptCount || 0;
  const attemptCount = previousAttempts + 1;

  try {
    await deliverRow(row, config);
    await writeAudit(row, config, {
      status: "SUCCESS",
      attemptCount,
      resolvedAt: new Date().toISOString(),
      error: null,
    });
    console.log(`  ✓ Delivered (attempt ${attemptCount}).`);
    return "SUCCESS";
  } catch (error) {
    const httpStatus = error.response?.status;
    const isGcsNotFound =
      error.message && error.message.includes("No such object");
    const isPermanent =
      error.isPermanent === true ||
      isGcsNotFound ||
      (httpStatus && PERMANENT_HTTP_STATUSES.includes(httpStatus));
    const hitMax = attemptCount >= MAX_ATTEMPTS;

    if (isPermanent || hitMax) {
      const reason = isPermanent ? "permanent_error" : "max_attempts_reached";
      await writeAudit(row, config, {
        status: "ABANDONED",
        attemptCount,
        abandonedAt: new Date().toISOString(),
        error: sanitizeError(error),
        abandonReason: reason,
      });
      await sendSlack("critical", row, config, error, reason, attemptCount);
      console.log(`  ⛔ Abandoned (${reason}).`);
      return "ABANDONED";
    }

    await writeAudit(row, config, {
      status: "PENDING_RETRY",
      attemptCount,
      error: sanitizeError(error),
    });
    if (attemptCount === WARN_AFTER_ATTEMPTS) {
      await sendSlack(
        "warning",
        row,
        config,
        error,
        "multiple_failures",
        attemptCount,
      );
    }
    console.log(
      `  ↻ Transient failure (attempt ${attemptCount}/${MAX_ATTEMPTS}).`,
    );
    return "RETRY_PENDING";
  }
}

async function deliverRow(row, config) {
  let fileId = "";

  if (config.hasImage) {
    if (!row.snapshot_url) {
      const err = new Error("Missing snapshot_url");
      err.stage = "pre_upload";
      err.isPermanent = true;
      throw err;
    }
    fileId = await uploadFile(row.snapshot_url);
    console.log(`  ✓ Uploaded: ${fileId}`);
  } else {
    console.log(`  ℹ No image for ${config.typeValue}, skipping upload.`);
  }

  const extraPayload = { id: row.id };
  for (const field of config.extraFields) {
    if (typeof field === "string") {
      extraPayload[field] = row[field];
    } else if (field && field.from && field.to) {
      extraPayload[field.to] = row[field.from];
    }
  }

  const payload = {
    cameraId: EDGE_ID,
    type: config.typeValue,
    detectedAt: toIsoTimestamp(row[config.timeField]),
    fileId: fileId,
    extra: [extraPayload],
  };

  await createViolation(payload);
  console.log(`  ✓ ${config.typeValue} violation created.`);
}

// ═══════════════════════════════════════════════════════════════════════
// API CALLS
// ═══════════════════════════════════════════════════════════════════════
async function uploadFile(snapshotUrl) {
  const url = `${BASE_URL}/services/eye/api/v2/webhooks/files`;
  const parts = snapshotUrl
    .replace("https://storage.googleapis.com/", "")
    .split("/");
  const bucketName = parts.shift();
  const fileName = parts.join("/");
  const original = fileName.split("/").pop();

  const gcsFile = storage.bucket(bucketName).file(fileName);
  const [meta] = await gcsFile.getMetadata();

  const form = new FormData();
  form.append("file", gcsFile.createReadStream(), {
    filename: original,
    knownLength: parseInt(meta.size, 10),
  });

  const res = await axios.post(url, form, {
    headers: {
      ...form.getHeaders(),
      "x-client-key": CLIENT_KEY,
      edgeId: EDGE_ID,
    },
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
  });

  return res.data.fileId || res.data.id || res.data.link;
}

async function createViolation(payload) {
  const url = `${BASE_URL}/services/eye/api/v1/webhooks/violations`;
  const res = await axios.post(url, payload, {
    headers: {
      "x-client-key": CLIENT_KEY,
      "Content-Type": "application/json",
      edgeId: EDGE_ID,
    },
  });
  return res.data;
}

// ═══════════════════════════════════════════════════════════════════════
// AUDIT
// ═══════════════════════════════════════════════════════════════════════
async function writeAudit(
  row,
  config,
  { status, attemptCount, error = null, ...rest },
) {
  const ref = firestore.collection(config.auditCollection).doc(row.id);
  const existing = await ref.get();

  // Compute detectedAt safely (frames config has no timeField)
  const detectedAt =
    (config.timeField &&
      row[config.timeField] &&
      toIsoTimestamp(row[config.timeField])) ||
    new Date().toISOString();

  const data = {
    rowId: row.id,
    cameraId: row.camera_id ?? null,
    moduleType: config.typeValue,
    detectedAt,
    branchId: row.branch_id ?? config.branchId ?? null,
    status,
    attemptCount,
    error,
    lastAttemptAt: new Date().toISOString(),
    ...rest,
  };

  if (!existing.exists) data.firstAttemptAt = data.lastAttemptAt;
  await ref.set(data, { merge: true });
}

async function getAuditDoc(collection, rowId) {
  const doc = await firestore.collection(collection).doc(rowId).get();
  return doc.exists ? doc.data() : null;
}

function sanitizeError(error) {
  return {
    message: error.message || "Unknown error",
    stage: error.stage || null,
    httpStatus: error.response?.status || null,
    errorCode:
      error.response?.data?.code || error.response?.data?.errorCode || null,
    endpoint: error.config?.url || null,
    responseBody: error.response?.data
      ? JSON.stringify(error.response.data).substring(0, 2000)
      : null,
  };
}

// ═══════════════════════════════════════════════════════════════════════
// SLACK
// ═══════════════════════════════════════════════════════════════════════
async function sendSlack(
  severity,
  row,
  config,
  error,
  reason,
  attemptCount = null,
) {
  if (!SLACK_WEBHOOK_URL) {
    console.log(`  [Slack disabled] ${severity}: ${row.id} — ${reason}`);
    return;
  }
  const emoji = severity === "critical" ? "🔴" : "🟡";
  const titleMap = {
    permanent_error: "Row ABANDONED — permanent API error",
    max_attempts_reached: `Row ABANDONED — ${MAX_ATTEMPTS} attempts exhausted`,
    multiple_failures: `Row failed ${attemptCount} times — will retry`,
  };
  const fields = [
    { title: "Row ID", value: row.id, short: true },
    { title: "Module", value: config.typeValue, short: true },
    { title: "Type", value: config.typeValue, short: true },
  ];
  if (error) {
    fields.push({
      title: "Stage",
      value: error.stage || "unknown",
      short: true,
    });
    fields.push({
      title: "HTTP",
      value: String(error.response?.status || "N/A"),
      short: true,
    });
    fields.push({
      title: "Error",
      value: (error.message || "").substring(0, 300),
      short: false,
    });
  }
  try {
    await axios.post(
      SLACK_WEBHOOK_URL,
      {
        text: `${emoji} *${titleMap[reason] || reason}* — Row \`${row.id}\``,
        attachments: [
          { color: severity === "critical" ? "#D93F3F" : "#F2C744", fields },
        ],
      },
      { timeout: 5000 },
    );
  } catch (err) {
    console.error("Slack failed:", err.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════
// WATERMARK
// ═══════════════════════════════════════════════════════════════════════
async function getWatermark(stateDocPath) {
  const ref = firestore.doc(stateDocPath);
  const doc = await ref.get();
  if (!doc.exists) {
    const initial = "1970-01-01T00:00:00.000Z";
    await ref.set({
      lastProcessedTimestamp: initial,
      updatedAt: new Date().toISOString(),
    });
    return initial;
  }
  const stored = doc.data().lastProcessedTimestamp;
  if (typeof stored === "string") return stored;
  if (stored?.toDate) return stored.toDate().toISOString();
  return new Date(stored).toISOString();
}

async function setWatermark(stateDocPath, timestamp) {
  const iso =
    typeof timestamp === "string"
      ? timestamp
      : new Date(timestamp).toISOString();
  await firestore
    .doc(stateDocPath)
    .set(
      { lastProcessedTimestamp: iso, updatedAt: new Date().toISOString() },
      { merge: true },
    );
  console.log(`💾 Watermark → ${iso}`);
}

// ═══════════════════════════════════════════════════════════════════════
// TIMESTAMP NORMALIZER
// ═══════════════════════════════════════════════════════════════════════
function toIsoTimestamp(rawValue) {
  if (rawValue == null) throw new Error("Timestamp is null");
  if (
    typeof rawValue === "object" &&
    !(rawValue instanceof Date) &&
    "value" in rawValue
  ) {
    return toIsoTimestamp(rawValue.value);
  }
  if (rawValue instanceof Date) return rawValue.toISOString();
  if (typeof rawValue === "string") {
    const p1 = dayjs.utc(rawValue, "YYYY-MM-DD HH:mm:ss.SSSSSS [UTC]", true);
    if (p1.isValid()) return p1.toISOString();
    const p2 = dayjs.utc(rawValue, "YYYY-MM-DD HH:mm:ss [UTC]", true);
    if (p2.isValid()) return p2.toISOString();
    const p3 = dayjs.utc(rawValue);
    if (p3.isValid()) return p3.toISOString();
  }
  throw new Error(`Cannot parse timestamp: ${JSON.stringify(rawValue)}`);
}
