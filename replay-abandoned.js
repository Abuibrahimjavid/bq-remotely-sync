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
const TABLE_ID = process.env.TABLE_ID;
const CLIENT_KEY = process.env.CLIENT_KEY;
const EDGE_ID = process.env.EDGE_ID;
const BASE_URL = process.env.BASE_URL;

const AUDIT_COLLECTION = process.env.AUDIT_COLLECTION || "sync_audit";

const ROW_ID = process.argv[2];

async function main() {
  if (!ROW_ID) {
    console.error("Usage: node replay-abandoned.js <rowId>");
    process.exit(1);
  }

  const auditRef = firestore.collection(AUDIT_COLLECTION).doc(ROW_ID);
  const auditDoc = await auditRef.get();
  if (!auditDoc.exists) {
    console.error(`No audit record for ${ROW_ID}`);
    process.exit(1);
  }

  const audit = auditDoc.data();
  if (audit.status !== "ABANDONED") {
    console.error(`Row is "${audit.status}", not ABANDONED. Nothing to do.`);
    process.exit(1);
  }

  console.log(`Replaying row ${ROW_ID}...`);

  const [rows] = await bigquery.query({
    query: `
      SELECT id, branch_id, camera_id, ppe_person_id, violation_type,
             violation_time, confidence, snapshot, snapshot_url
      FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
      WHERE id = @rowId LIMIT 1
    `,
    params: { rowId: ROW_ID },
  });

  if (rows.length === 0) {
    console.error(`Row not found in BigQuery.`);
    process.exit(1);
  }

  const row = rows[0];

  try {
    const fileId = await uploadFile(row.snapshot_url);
    console.log(`  ✓ Uploaded: ${fileId}`);

    await createViolation({
      cameraId: row.camera_id,
      type: row.violation_type,
      detectedAt: toIsoTimestamp(row.violation_time),
      fileId,
      extra: [
        {
          id: row.id, // <-- Added: To match index.js
          snapshot: row.snapshot, // <-- Added: To match index.js
          camera_id: row.camera_id, // <-- ADDED
          confidence: row.confidence,
          ppe_person_id: row.ppe_person_id,
          branch_id: row.branch_id,
        },
      ],
    });
    console.log(`  ✓ Violation created.`);

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

// ─── Shared helpers (duplicated on purpose) ──────────────────────────
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
