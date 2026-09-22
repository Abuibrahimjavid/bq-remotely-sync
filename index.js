const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');
const axios = require('axios');
const FormData = require('form-data');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const customParseFormat = require('dayjs/plugin/customParseFormat');

dayjs.extend(utc);
dayjs.extend(customParseFormat);

const bigquery = new BigQuery();
const storage = new Storage();

// --- Configuration ---
const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const TABLE_ID = process.env.TABLE_ID;
const CLIENT_KEY = process.env.CLIENT_KEY; 
const EDGE_ID = process.env.EDGE_ID; 

exports.processNewRows = async (req, res) => {
    try {
        console.log("Starting BigQuery to API sync...");
        const lastProcessedTimestamp = await getLastProcessedTimestamp();
        
        const query = `
            SELECT id, branch_id, camera_id, ppe_person_id, violation_type, 
                   violation_time, confidence, snapshot, snapshot_url, ingested_at
            FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
            WHERE ingested_at > @lastTimestamp
            ORDER BY ingested_at ASC
            LIMIT 50
        `;

        const options = {
            query: query,
            params: { lastTimestamp: lastProcessedTimestamp },
        };

        const [rows] = await bigquery.query(options);
        console.log(`Found ${rows.length} new rows to process.`);

        for (const row of rows) {
            await processSingleViolation(row);
            await updateLastProcessedTimestamp(row.ingested_at.value);
        }

        res.status(200).send(`Successfully processed ${rows.length} rows.`);
    } catch (error) {
        console.error("Error in sync process:", error);
        res.status(500).send("Internal Server Error");
    }
};

async function processSingleViolation(row) {
    try {
        let fileId = null;
        if (row.snapshot_url) {
            fileId = await uploadFile(row.snapshot_url, row.id);
        }

        if (fileId) {
            // Use dayjs to parse the BigQuery timestamp string
            const detectedAt = dayjs.utc(row.violation_time.value, "YYYY-MM-DD HH:mm:ss.SSSSSS [UTC]").toISOString();

            const payload = {
                cameraId: row.camera_id, // Sending "04" as-is
                type: row.violation_type, // Sending "NO_MASK" as-is
                detectedAt: detectedAt,
                fileId: fileId,
                extra: [{
                    confidence: row.confidence,
                    ppe_person_id: row.ppe_person_id,
                    branch_id: row.branch_id
                }]
            };
            
            await createViolation(payload);
            console.log(`Violation created successfully for row ${row.id}`);
        }
    } catch (error) {
        console.error(`Error processing row ${row.id}:`, error.message);
        throw error; 
    }
}

async function uploadFile(snapshotUrl, rowId) {
    const url = `${BASE_URL}/services/eye/api/v2/webhooks/files`;
    
    // Parse the GCS URL
    const urlParts = snapshotUrl.replace('https://storage.googleapis.com/', '').split('/');
    const bucketName = urlParts.shift();
    const fileName = urlParts.join('/');

    console.log(`Downloading ${fileName} from bucket ${bucketName}...`);
    const [fileBuffer] = await storage.bucket(bucketName).file(fileName).download();

    const form = new FormData();
    const originalFileName = fileName.split('/').pop();
    form.append('video/image', fileBuffer, { filename: originalFileName });

    const response = await axios.post(url, form, {
        headers: {
            ...form.getHeaders(),
            'x-client-key': CLIENT_KEY, 
            'edgeId': EDGE_ID 
        }
    });

    return response.data.fileId || response.data.id; 
}

async function createViolation(payload) {
    const url = `${BASE_URL}/services/eye/api/v1/webhooks/violations`;
    
    const response = await axios.post(url, payload, {
        headers: {
            'x-client-key': CLIENT_KEY,
            'Content-Type': 'application/json',
            'edgeId': EDGE_ID
        }
    });

    return response.data;
}

// --- State Management Helpers ---
async function getLastProcessedTimestamp() {
    return "1970-01-01T00:00:00Z"; 
}

async function updateLastProcessedTimestamp(timestamp) {
    console.log(`Updating state to: ${timestamp}`);
}