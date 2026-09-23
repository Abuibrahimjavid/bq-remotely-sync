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

// --- Initialize GCP Clients ---
const bigquery = new BigQuery();
const storage = new Storage();
const firestore = new Firestore();

// --- Configuration ---
const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const TABLE_ID = process.env.TABLE_ID;
const CLIENT_KEY = process.env.CLIENT_KEY;
const EDGE_ID = process.env.EDGE_ID;
const BASE_URL = process.env.BASE_URL;

const STATE_DOC_PATH = "pipeline_state/sync_job";

const DRY_RUN = process.env.DRY_RUN === "true";

/**
 * 🔑 CENTRAL TIMESTAMP NORMALIZER
 * Converts any timestamp shape returned by BigQuery into a valid ISO 8601 string.
 *
 * Handles:
 *  1. BigQuery wrapper object: { value: "2026-09-22 11:07:12.995552 UTC" }
 *  2. Native JS Date:          new Date("2026-09-22T11:07:12.995Z")
 *  3. BigQuery raw string:     "2026-09-22 11:07:12.995552 UTC"
 *  4. Already ISO string:      "2026-09-22T11:07:12.995Z"
 *
 * Returns: "2026-09-22T11:07:12.995Z" (millisecond precision, UTC)
 * Throws: if it cannot parse the input at all.
 */
function toIsoTimestamp(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    throw new Error("Timestamp value is null or undefined");
  }

  // Case 1: BigQuery wrapper object → unwrap and recurse
  if (
    typeof rawValue === "object" &&
    !(rawValue instanceof Date) &&
    "value" in rawValue
  ) {
    return toIsoTimestamp(rawValue.value);
  }

  // Case 2: Native JS Date → direct ISO
  if (rawValue instanceof Date) {
    return rawValue.toISOString();
  }

  // Case 3: String input (BigQuery raw string or ISO)
  if (typeof rawValue === "string") {
    // Try the exact BigQuery format first: "2026-09-22 11:07:12.995552 UTC"
    const parsed = dayjs.utc(
      rawValue,
      "YYYY-MM-DD HH:mm:ss.SSSSSS [UTC]",
      true,
    );
    if (parsed.isValid()) return parsed.toISOString();

    // Fallback: some rows might have fewer fractional digits
    const parsed2 = dayjs.utc(rawValue, "YYYY-MM-DD HH:mm:ss [UTC]", true);
    if (parsed2.isValid()) return parsed2.toISOString();

    // Fallback: standard ISO string (from Firestore or elsewhere)
    const parsed3 = dayjs.utc(rawValue);
    if (parsed3.isValid()) return parsed3.toISOString();
  }

  throw new Error(`Unable to parse timestamp: ${JSON.stringify(rawValue)}`);
}

exports.processNewRows = async (req, res) => {
    try {
        console.log("🚀 Starting BigQuery to API sync...");

        const lastProcessedTimestamp = await getLastProcessedTimestamp();
        console.log(`📅 Fetching rows newer than: ${lastProcessedTimestamp}`);

        const query = `
            SELECT id, branch_id, camera_id, ppe_person_id, violation_type,
                   violation_time, confidence, snapshot, snapshot_url
            FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
            WHERE violation_time > @lastTimestamp
            ORDER BY violation_time ASC
            LIMIT 50
        `;

        // 🔑 FIXED: Pass a real JS Date object — the SDK infers TIMESTAMP correctly
        const options = {
            query: query,
            params: {
                lastTimestamp: new Date(lastProcessedTimestamp)
            }
        };

        // 🔍 DIAGNOSTIC
        console.log("🔍 DIAGNOSTIC — Query being sent:");
        console.log("   Query:", query.replace(/\s+/g, " ").trim());
        console.log("   Param:", options.params.lastTimestamp, "| type:", typeof options.params.lastTimestamp);

        const [rows] = await bigquery.query(options);
        console.log(`📊 Found ${rows.length} new rows to process.`);

        if (rows.length === 0) {
            return res.status(200).send("No new rows to process.");
        }

        for (const row of rows) {
            await processSingleViolation(row);

            const isoTime = toIsoTimestamp(row.violation_time);

            if (DRY_RUN) {
                console.log(`  🧪 [DRY RUN] Would update state to: ${isoTime}`);
            } else {
                await updateLastProcessedTimestamp(isoTime);
            }
        }

        res.status(200).send(`✅ Successfully processed ${rows.length} rows.`);
    } catch (error) {
        console.error("❌ Error in sync process:", error);
        res.status(500).send("Internal Server Error");
    }
};

async function processSingleViolation(row) {
  try {
    console.log(`🔹 Processing row ID: ${row.id}`);

    let fileId = null;
    if (row.snapshot_url) {
      if (DRY_RUN) {
        // 🔍 DRY RUN: fake a fileId based on the real filename
        const fakeFileName = row.snapshot_url.split("/").pop();
        fileId = `DRY_RUN_FILE_ID_${fakeFileName}`;
        console.log(`  🧪 [DRY RUN] Would upload: ${row.snapshot_url}`);
        console.log(`  🧪 [DRY RUN] Fake fileId: ${fileId}`);
      } else {
        fileId = await uploadFile(row.snapshot_url, row.id);
        console.log(`  ✓ File uploaded. fileId: ${fileId}`);
      }
    } else {
      console.warn(`  ⚠ No snapshot_url for row ${row.id}. Skipping upload.`);
      return;
    }

    const detectedAt = toIsoTimestamp(row.violation_time);

    const payload = {
      cameraId: row.camera_id,
      type: row.violation_type,
      detectedAt: detectedAt,
      fileId: fileId,
      extra: [
        {
          confidence: row.confidence,
          ppe_person_id: row.ppe_person_id,
          branch_id: row.branch_id,
        },
      ],
    };

    if (DRY_RUN) {
      console.log(`  🧪 [DRY RUN] Would POST violation with payload:`);
      console.log(JSON.stringify(payload, null, 2));
    } else {
      await createViolation(payload);
      console.log(
        `  ✓ Violation created for row ${row.id} (detectedAt: ${detectedAt})`,
      );
    }
  } catch (error) {
    console.error(`❌ Error processing row ${row.id}:`, error.message);
    throw error;
  }
}

async function uploadFile(snapshotUrl, rowId) {
  const url = `${BASE_URL}/services/eye/api/v2/webhooks/files`;

  const urlParts = snapshotUrl
    .replace("https://storage.googleapis.com/", "")
    .split("/");
  const bucketName = urlParts.shift();
  const fileName = urlParts.join("/");
  const originalFileName = fileName.split("/").pop();

  console.log(`  ⬇ Streaming gs://${bucketName}/${fileName}`);

  const gcsFile = storage.bucket(bucketName).file(fileName);
  const [metadata] = await gcsFile.getMetadata();
  const fileSize = parseInt(metadata.size, 10);
  const readStream = gcsFile.createReadStream();

  const form = new FormData();
  form.append("file", readStream, {
    filename: originalFileName,
    knownLength: fileSize,
  });

  const response = await axios.post(url, form, {
    headers: {
      ...form.getHeaders(),
      "x-client-key": CLIENT_KEY,
      edgeId: EDGE_ID,
    },
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
  });

  return response.data.fileId || response.data.id || response.data.link;
}

async function createViolation(payload) {
  const url = `${BASE_URL}/services/eye/api/v1/webhooks/violations`;

  const response = await axios.post(url, payload, {
    headers: {
      "x-client-key": CLIENT_KEY,
      "Content-Type": "application/json",
      edgeId: EDGE_ID,
    },
  });

  return response.data;
}

// --- State Management (Firestore) ---

async function getLastProcessedTimestamp() {
  const docRef = firestore.doc(STATE_DOC_PATH);
  const doc = await docRef.get();

  if (!doc.exists) {
    console.log("📝 No state document found. Initializing...");
    const defaultTimestamp = "1970-01-01T00:00:00.000Z";
    await docRef.set({
      lastProcessedTimestamp: defaultTimestamp,
      updatedAt: new Date().toISOString(),
    });
    return defaultTimestamp;
  }

  const stored = doc.data().lastProcessedTimestamp;

  // 🔑 Always return an ISO STRING so BigQuery params stay consistent
  if (typeof stored === "string") return stored;
  if (stored && typeof stored.toDate === "function")
    return stored.toDate().toISOString();
  if (stored instanceof Date) return stored.toISOString();

  // Last resort: coerce to string
  return new Date(stored).toISOString();
}

async function updateLastProcessedTimestamp(timestamp) {
  // 🔑 Force ISO string — never store Date / Firestore Timestamp objects
  const isoTimestamp =
    typeof timestamp === "string"
      ? timestamp
      : new Date(timestamp).toISOString();

  const docRef = firestore.doc(STATE_DOC_PATH);
  await docRef.set(
    {
      lastProcessedTimestamp: isoTimestamp,
      updatedAt: new Date().toISOString(),
    },
    { merge: true },
  );

  console.log(`💾 State updated to: ${isoTimestamp}`);
}
