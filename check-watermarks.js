require("dotenv").config();
const { Firestore } = require("@google-cloud/firestore");

const firestore = new Firestore();

const JOBS = [
  "ppe_job",
  "footfall_job",
  "heatmap_job",
  "staff_occupancy_job",
  "heatmap_frames_job",
];

async function main() {
  console.log("\n═══════════════════════════════════════════════════════════════");
  console.log("  PIPELINE WATERMARKS");
  console.log("═══════════════════════════════════════════════════════════════\n");

  const now = new Date();

  for (const jobId of JOBS) {
    const doc = await firestore.doc(`pipeline_state/${jobId}`).get();

    if (!doc.exists) {
      console.log(`  ❌ ${jobId.padEnd(28)} NOT INITIALIZED`);
      continue;
    }

    const data = doc.data();

    // Regular watermark
    if (data.lastProcessedTimestamp) {
      const ts = new Date(data.lastProcessedTimestamp);
      const ageMin = Math.round((now - ts) / 60000);
      const ageStr =
        ageMin < 60
          ? `${ageMin}m ago`
          : ageMin < 1440
            ? `${Math.round(ageMin / 60)}h ago`
            : `${Math.round(ageMin / 1440)}d ago`;

      console.log(`  ✅ ${jobId.padEnd(28)} → ${data.lastProcessedTimestamp}  (${ageStr})`);
    }

    // Frame-specific date
    if (data.lastFrameDate) {
      console.log(`  ✅ ${jobId.padEnd(28)} → lastFrameDate: ${data.lastFrameDate}`);
    }

    console.log(`     updatedAt: ${data.updatedAt}`);
    console.log("");
  }

  console.log("═══════════════════════════════════════════════════════════════\n");
}

main().catch(console.error);