require("dotenv").config();
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

const bigquery = new BigQuery();
const storage = new Storage();
const firestore = new Firestore();

const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const CLIENT_KEY = process.env.CLIENT_KEY;
const EDGE_ID = process.env.EDGE_ID;
const BASE_URL = process.env.BASE_URL;

// ═══════════════════════════════════════════════════════════════════════
// Same DATASET_CONFIG as index.js — duplicated for self-containment
// ═══════════════════════════════════════════════════════════════════════
const DATASET_CONFIG = {
  ppe: {
    tableId: "namaq_ppe",
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
      "camera_id",
      "branch_id",
    ],
  },
  footfall: {
    tableId: "namaq_footfall",
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
    extraFields: ["track_id", "direction", "camera_id", "branch_id"],
  },
  heatmap: {
    tableId: "namaq_heatmap",
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
    extraFields: ["x", "y", "track_id", "camera_id", "branch_id"],
  },
  staff_occupancy: {
    // <-- ADD THIS
    tableId: "namaq_staff_occupancy", // <-- ADD THIS
    auditCollection: "sync_audit_staff_occupancy", // <-- ADD THIS
    timeField: "event_time", // <-- ADD THIS
    typeValue: "STAFF_OCCUPANCY", // <-- ADD THIS
    hasImage: false, // <-- ADD THIS
    selectFields: ["id", "branch_id", "camera_id", "staff_count", "event_time"], // <-- ADD THIS
    extraFields: ["staff_count", "camera_id", "branch_id"], // <-- ADD THIS
  }, // <-- ADD THIS
};

const ROW_ID = process.argv[2];
const MODE = (process.argv[3] || "").toLowerCase();

async function main() {
  if (!ROW_ID || !MODE) {
    console.error("Usage: node replay-abandoned.js <rowId> <mode>");
    console.error("Modes: ppe | footfall | heatmap");
    process.exit(1);
  }

  const config = DATASET_CONFIG[MODE];
  if (!config) {
    console.error(`Invalid mode "${MODE}". Use: ppe | footfall | heatmap`);
    process.exit(1);
  }

  const auditRef = firestore.collection(config.auditCollection).doc(ROW_ID);
  const auditDoc = await auditRef.get();
  if (!auditDoc.exists) {
    console.error(`No audit record for ${ROW_ID} in ${config.auditCollection}`);
    process.exit(1);
  }

  const audit = auditDoc.data();
  if (audit.status !== "ABANDONED") {
    console.error(`Row is "${audit.status}", not ABANDONED. Nothing to do.`);
    process.exit(1);
  }

  console.log(`Replaying ${MODE.toUpperCase()} row ${ROW_ID}...`);

  const selectList = config.selectFields.join(", ");
  const [rows] = await bigquery.query({
    query: `
      SELECT ${selectList}
      FROM \`${PROJECT_ID}.${DATASET_ID}.${config.tableId}\`
      WHERE id = @rowId LIMIT 1
    `,
    params: { rowId: ROW_ID },
  });

  if (rows.length === 0) {
    console.error(`Row not found in ${config.tableId}.`);
    process.exit(1);
  }

  const row = rows[0];

  try {
    let fileId = "";
    if (config.hasImage) {
      if (!row.snapshot_url) {
        throw new Error("Missing snapshot_url");
      }
      fileId = await uploadFile(row.snapshot_url);
      console.log(`  ✓ Uploaded: ${fileId}`);
    } else {
      console.log(`  ℹ No image for ${config.typeValue}, skipping upload.`);
    }

    const extraPayload = { id: row.id };
    for (const field of config.extraFields) {
      extraPayload[field] = row[field];
    }

    await createViolation({
      cameraId: EDGE_ID,
      type: config.typeValue,
      detectedAt: toIsoTimestamp(row[config.timeField]),
      fileId: fileId,
      extra: [extraPayload],
    });
    console.log(`  ✓ ${config.typeValue} violation created.`);

    await auditRef.set(
      {
        status: "SUCCESS",
        resolvedAt: new Date().toISOString(),
        resolvedBy: "manual_replay",
        error: null,
      },
      { merge: true },
    );

    console.log(`✅ Row ${ROW_ID} marked SUCCESS.`);
  } catch (err) {
    console.error(`❌ Replay failed: ${err.message}`);
    await auditRef.set(
      {
        status: "ABANDONED",
        error: {
          message: err.message,
          httpStatus: err.response?.status || null,
        },
        abandonReason: "manual_replay_failed",
        lastAttemptAt: new Date().toISOString(),
      },
      { merge: true },
    );
    process.exit(1);
  }
}

// ─── Shared helpers ──────────────────────────────────────────────────
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

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
