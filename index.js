const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');
const FormData = require('form-data');
const { Storage } = require('@google-cloud/storage'); // ADD THIS
const storage = new Storage(); // ADD THIS

// Initialize BigQuery client
const bigquery = new BigQuery();

// --- Configuration ---
const PROJECT_ID = "crack-will-451616-g4";
const DATASET_ID = "si_bulbul_live";
const TABLE_ID = "bulbul_ppe";

// UPDATE: Use environment variables for production. 
// The doc says "The X-client-key will be generated for each business."
const CLIENT_KEY = process.env.CLIENT_KEY || "pLbwvD0bC8P56L94pBTQR86yNrvHqmzF7LxKJI2t"; 
const EDGE_ID = process.env.EDGE_ID || "0f872C07-1fec-485f-895f-37f0b10e3229"; 
const BASE_URL = "https://app.remotely.store";

/**
 * Main entry point for the Cloud Run function.
 */
exports.processNewRows = async (req, res) => {
    try {
        console.log("Starting BigQuery to API sync...");
        
        // 1. Get the last processed timestamp from your state store
        const lastProcessedTimestamp = await getLastProcessedTimestamp();
        console.log(`Fetching rows newer than: ${lastProcessedTimestamp}`);

        // 2. Query BigQuery for new rows
        const query = `
            SELECT id, branch_id, camera_id, ppe_person_id, violation_type, 
                   violation_time, confidence, snapshot, snapshot_url, ingested_at
            FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
            WHERE ingested_at > @lastTimestamp
            ORDER BY ingested_at ASC
        `;

        const options = {
            query: query,
            params: { lastTimestamp: lastProcessedTimestamp },
        };

        const [rows] = await bigquery.query(options);
        console.log(`Found ${rows.length} new rows to process.`);

        // 3. Process each row
        for (const row of rows) {
            await processSingleViolation(row);
            // Update state after each successful row to avoid re-processing
            await updateLastProcessedTimestamp(row.ingested_at.value);
        }

        res.status(200).send(`Successfully processed ${rows.length} rows.`);
    } catch (error) {
        console.error("Error in sync process:", error);
        res.status(500).send("Internal Server Error");
    }
};

/**
 * Handles the upload and violation creation for a single row.
 */
async function processSingleViolation(row) {
    try {
        console.log(`Processing row ID: ${row.id}`);

        let fileId = null;
        // UPDATE: We need to check snapshot_url, not snapshot
        if (row.snapshot_url) {
            // Pass the URL to the upload function
            fileId = await uploadFile(row.snapshot_url, row.id);
            console.log(`File uploaded successfully. File ID: ${fileId}`);
        } else {
            console.warn(`No snapshot URL found for row ${row.id}. Skipping file upload.`);
        }

        // ... rest of the payload creation ...
    } catch (error) {
        // ...
    }
}

/**
 * Calls the /files upload endpoint.
 * @param {string} snapshotUrl - The full GCS URL (e.g., https://storage.googleapis.com/bucket/path/file.jpg)
 * @param {string} rowId - The BigQuery row ID for naming the file
 */
async function uploadFile(snapshotUrl, rowId) {
    const url = `${BASE_URL}/services/eye/api/v2/webhooks/files`;
    
    // 1. Parse the GCS URL to get bucket and file path
    // Expected format: https://storage.googleapis.com/BUCKET_NAME/FILE_PATH
    const urlParts = snapshotUrl.replace('https://storage.googleapis.com/', '').split('/');
    const bucketName = urlParts.shift(); // "si-bulbul-live-bucket"
    const fileName = urlParts.join('/');   // "snapshots/camera_04/no_mask_id3850_20260825_111037.jpg"

    console.log(`Downloading ${fileName} from bucket ${bucketName}...`);

    // 2. Download the file into a Buffer
    const [fileBuffer] = await storage.bucket(bucketName).file(fileName).download();

    // 3. Prepare the FormData
    const form = new FormData();
    // Use the original filename from the URL
    const originalFileName = fileName.split('/').pop();
    form.append('video/image', fileBuffer, { filename: originalFileName });

    // 4. Send to the API
    const response = await axios.post(url, form, {
        headers: {
            ...form.getHeaders(),
            'x-client-key': CLIENT_KEY, 
            'edgeId': EDGE_ID 
        }
    });

    return response.data.fileId || response.data.id; 
}

/**
 * Calls the /violations endpoint.
 */
async function createViolation(payload) {
    const url = `${BASE_URL}/services/eye/api/v1/webhooks/violations`;
    
    const response = await axios.post(url, payload, {
        headers: {
            // UPDATE: Header changed from 'client-key' to 'x-client-key'
            'x-client-key': CLIENT_KEY,
            'Content-Type': 'application/json',
            // UPDATE: Added required 'edgeId' header
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