if (process.env.NODE_ENV !== "production") {
  require("dotenv").config();
}

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

// ─── Config ──────────────────────────────────────────────────────────
const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const TABLE_ID = process.env.TABLE_ID;
const CLIENT_KEY = process.env.CLIENT_KEY;
const EDGE_ID = process.env.EDGE_ID;
const BASE_URL = process.env.BASE_URL;
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL || "";

// ─── Retry Policy ────────────────────────────────────────────────────
const MAX_ATTEMPTS = 10;              // give up after 10 runs (~50 min)
const WARN_AFTER_ATTEMPTS = 5;        // Slack warning fires once, at this attempt
const PERMANENT_HTTP_STATUSES = [400, 401, 403, 404, 405, 410, 422];

// ─── Firestore Paths ─────────────────────────────────────────────────
const STATE_DOC_PATH = "pipeline_state/sync_job";
const AUDIT_COLLECTION = "sync_audit";

// ═══════════════════════════════════════════════════════════════════════
// ENTRY POINT
// ═══════════════════════════════════════════════════════════════════════
exports.processNewRows = async (req, res) => {
  try {
    console.log("🚀 Starting BigQuery to Eye API sync...");

    const watermark = await getWatermark();
    console.log(`📅 Watermark: ${watermark}`);

    const [rows] = await bigquery.query({
      query: `
        SELECT id, branch_id, camera_id, ppe_person_id, violation_type,
               violation_time, confidence, snapshot, snapshot_url
        FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
        WHERE violation_time > @watermark
        ORDER BY violation_time ASC
        LIMIT 50
      `,
      params: { watermark: new Date(watermark) },
    });

    console.log(`📊 Found ${rows.length} rows.`);
    if (rows.length === 0) {
      return res.status(200).send("No new rows.");
    }

    let succeeded = 0, abandoned = 0, pendingRetry = 0;

    for (const row of rows) {
      const result = await processRow(row);
      const isoTime = toIsoTimestamp(row.violation_time);

      if (result === "SUCCESS" || result === "ABANDONED") {
        await setWatermark(isoTime);
        if (result === "SUCCESS") succeeded++; else abandoned++;

      } else if (result === "RETRY_PENDING") {
        pendingRetry++;
        console.log(`  ⏸ Row ${row.id} pending retry. Stopping batch.`);
        break;
      }
    }

    return res.status(200).send(
      `✅ Processed: ${succeeded} ok, ${abandoned} abandoned, ${pendingRetry} pending-retry.`
    );

  } catch (err) {
    console.error("❌ Fatal error:", err);
    return res.status(500).send("Internal Server Error");
  }
};

// ═══════════════════════════════════════════════════════════════════════
// ROW PROCESSING
// ═══════════════════════════════════════════════════════════════════════
/**
 * Returns: "SUCCESS" | "ABANDONED" | "RETRY_PENDING"
 */
async function processRow(row) {
  console.log(`🔹 Row ${row.id}`);

  const existingAudit = await getAuditDoc(row.id);
  const previousAttempts = existingAudit?.attemptCount || 0;
  const attemptCount = previousAttempts + 1;

  try {
    await deliverRow(row);
    await writeAudit(row, {
      status: "SUCCESS",
      attemptCount,
      resolvedAt: new Date().toISOString(),
      error: null,
    });
    console.log(`  ✓ Delivered (attempt ${attemptCount}).`);
    return "SUCCESS";

  } catch (error) {
    const httpStatus = error.response?.status;
    const isPermanent =
      error.isPermanent === true ||
      (httpStatus && PERMANENT_HTTP_STATUSES.includes(httpStatus));
    const hitMax = attemptCount >= MAX_ATTEMPTS;

    if (isPermanent || hitMax) {
      const reason = isPermanent ? "permanent_error" : "max_attempts_reached";
      await writeAudit(row, {
        status: "ABANDONED",
        attemptCount,
        abandonedAt: new Date().toISOString(),
        error: sanitizeError(error),
        abandonReason: reason,
      });
      await sendSlack("critical", row, error, reason, attemptCount);
      console.log(`  ⛔ Abandoned (${reason}).`);
      return "ABANDONED";
    }

    await writeAudit(row, {
      status: "PENDING_RETRY",
      attemptCount,
      error: sanitizeError(error),
    });

    // Warn only once, exactly at WARN_AFTER_ATTEMPTS — not on every attempt after
    if (attemptCount === WARN_AFTER_ATTEMPTS) {
      await sendSlack("warning", row, error, "multiple_failures", attemptCount);
    }
    console.log(`  ↻ Transient failure (attempt ${attemptCount}/${MAX_ATTEMPTS}).`);
    return "RETRY_PENDING";
  }
}

async function deliverRow(row) {
  if (!row.snapshot_url) {
    const err = new Error("Missing snapshot_url");
    err.stage = "pre_upload";
    err.isPermanent = true;   // ← no point retrying, skip immediately
    throw err;
  }

  const fileId = await uploadFile(row.snapshot_url);
  console.log(`  ✓ Uploaded: ${fileId}`);

  const payload = {
    cameraId: row.camera_id,
    type: row.violation_type,
    detectedAt: toIsoTimestamp(row.violation_time),
    fileId,
    extra: [{
      confidence: row.confidence,
      ppe_person_id: row.ppe_person_id,
      branch_id: row.branch_id,
    }],
  };

  await createViolation(payload);
  console.log(`  ✓ Violation created.`);
}

// ═══════════════════════════════════════════════════════════════════════
// API CALLS
// ═══════════════════════════════════════════════════════════════════════
async function uploadFile(snapshotUrl) {
  const url = `${BASE_URL}/services/eye/api/v2/webhooks/files`;

  const parts = snapshotUrl.replace("https://storage.googleapis.com/", "").split("/");
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
async function writeAudit(row, { status, attemptCount, error = null, ...rest }) {
  const ref = firestore.collection(AUDIT_COLLECTION).doc(row.id);
  const existing = await ref.get();

  const data = {
    rowId: row.id,
    cameraId: row.camera_id,
    violationType: row.violation_type,
    detectedAt: toIsoTimestamp(row.violation_time),
    snapshotUrl: row.snapshot_url || null,
    branchId: row.branch_id,
    status,
    attemptCount,
    error,
    lastAttemptAt: new Date().toISOString(),
    ...rest,
  };

  if (!existing.exists) {
    data.firstAttemptAt = data.lastAttemptAt;
  }

  await ref.set(data, { merge: true });
}

async function getAuditDoc(rowId) {
  const doc = await firestore.collection(AUDIT_COLLECTION).doc(rowId).get();
  return doc.exists ? doc.data() : null;
}

function sanitizeError(error) {
  return {
    message: error.message || "Unknown error",
    stage: error.stage || null,
    httpStatus: error.response?.status || null,
    errorCode: error.response?.data?.code || error.response?.data?.errorCode || null,
    endpoint: error.config?.url || null,
    responseBody: error.response?.data
      ? JSON.stringify(error.response.data).substring(0, 2000)
      : null,
  };
}

// ═══════════════════════════════════════════════════════════════════════
// SLACK
// ═══════════════════════════════════════════════════════════════════════
async function sendSlack(severity, row, error, reason, attemptCount = null) {
  if (!SLACK_WEBHOOK_URL) {
    console.log(`  [Slack disabled] ${severity}: ${row.id} — ${reason}`);
    return;
  }

  const emoji = severity === "critical" ? "🔴" : "🟡";
  const titleMap = {
    permanent_error:      "Row ABANDONED — permanent API error",
    max_attempts_reached: `Row ABANDONED — ${MAX_ATTEMPTS} attempts exhausted`,
    multiple_failures:    `Row failed ${attemptCount} times — will retry`,
  };

  const fields = [
    { title: "Row ID",   value: row.id,                             short: true },
    { title: "Camera",   value: String(row.camera_id),              short: true },
    { title: "Type",     value: row.violation_type,                 short: true },
    { title: "Detected", value: toIsoTimestamp(row.violation_time), short: true },
  ];
  if (error) {
    fields.push({ title: "Stage", value: error.stage || "unknown", short: true });
    fields.push({ title: "HTTP",  value: String(error.response?.status || "N/A"), short: true });
    fields.push({ title: "Error", value: (error.message || "").substring(0, 300), short: false });
  }

  try {
    await axios.post(SLACK_WEBHOOK_URL, {
      text: `${emoji} *${titleMap[reason] || reason}* — Row \`${row.id}\``,
      attachments: [{ color: severity === "critical" ? "#D93F3F" : "#F2C744", fields }],
    }, { timeout: 5000 });
  } catch (err) {
    console.error("Slack failed:", err.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════
// WATERMARK
// ═══════════════════════════════════════════════════════════════════════
async function getWatermark() {
  const ref = firestore.doc(STATE_DOC_PATH);
  const doc = await ref.get();

  if (!doc.exists) {
    const initial = "1970-01-01T00:00:00.000Z";
    await ref.set({ lastProcessedTimestamp: initial, updatedAt: new Date().toISOString() });
    return initial;
  }

  const stored = doc.data().lastProcessedTimestamp;
  if (typeof stored === "string") return stored;
  if (stored?.toDate) return stored.toDate().toISOString();
  return new Date(stored).toISOString();
}

async function setWatermark(timestamp) {
  const iso = typeof timestamp === "string" ? timestamp : new Date(timestamp).toISOString();
  await firestore.doc(STATE_DOC_PATH).set(
    { lastProcessedTimestamp: iso, updatedAt: new Date().toISOString() },
    { merge: true }
  );
  console.log(`💾 Watermark → ${iso}`);
}

// ═══════════════════════════════════════════════════════════════════════
// TIMESTAMP NORMALIZER
// ═══════════════════════════════════════════════════════════════════════
function toIsoTimestamp(rawValue) {
  if (rawValue == null) throw new Error("Timestamp is null");
  if (typeof rawValue === "object" && !(rawValue instanceof Date) && "value" in rawValue) {
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