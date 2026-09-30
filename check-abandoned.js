require("dotenv").config();
const { Firestore } = require("@google-cloud/firestore");

const firestore = new Firestore();

const COLLECTIONS = [
  "sync_audit_ppe",
  "sync_audit_footfall",
  "sync_audit_heatmap",
  "sync_audit_staff_occupancy",
  "sync_audit_table_cleaning",
  "sync_audit_heatmap_frames",
];

async function main() {
  const showDetails = process.argv.includes("--details");
  const modeArg = process.argv[2] && !process.argv[2].startsWith("--")
    ? process.argv[2].replace("sync_audit_", "").replace("all", "")
    : null;

  console.log("\n═══════════════════════════════════════════════════════════════");
  console.log("  AUDIT STATUS SUMMARY");
  console.log("═══════════════════════════════════════════════════════════════\n");

  const cols = modeArg
    ? COLLECTIONS.filter((c) => c.includes(modeArg))
    : COLLECTIONS;

  for (const col of cols) {
    const successSnap = await firestore
      .collection(col)
      .where("status", "==", "SUCCESS")
      .count()
      .get();

    const abandonedSnap = await firestore
      .collection(col)
      .where("status", "==", "ABANDONED")
      .count()
      .get();

    const pendingSnap = await firestore
      .collection(col)
      .where("status", "==", "PENDING_RETRY")
      .count()
      .get();

    const s = successSnap.data().count;
    const a = abandonedSnap.data().count;
    const p = pendingSnap.data().count;
    const total = s + a + p;
    const rate = total > 0 ? ((a / total) * 100).toFixed(2) : "0.00";

    const flag = a === 0 ? "✅" : a / total > 0.3 ? "🛑" : a / total > 0.05 ? "🟡" : "🟢";

    console.log(`  ${flag} ${col}`);
    console.log(`      SUCCESS:        ${s}`);
    console.log(`      ABANDONED:      ${a}  (${rate}%)`);
    console.log(`      PENDING_RETRY:  ${p}`);
    console.log(`      TOTAL:          ${total}`);
    console.log("");
  }

  // Show detailed abandoned list for a specific collection
  if (showDetails && modeArg) {
    const col = `sync_audit_${modeArg}`;
    console.log("═══════════════════════════════════════════════════════════════");
    console.log(`  ABANDONED DETAILS — ${col}`);
    console.log("═══════════════════════════════════════════════════════════════\n");

    const snap = await firestore
      .collection(col)
      .where("status", "==", "ABANDONED")
      .limit(20)
      .get();

    if (snap.empty) {
      console.log("  No abandoned rows.\n");
    } else {
      snap.docs.forEach((doc, i) => {
        const d = doc.data();
        console.log(`  [${i + 1}] ${doc.id}`);
        console.log(`      detectedAt:     ${d.detectedAt}`);
        console.log(`      errorCode:      ${d.error?.errorCode || "N/A"}`);
        console.log(`      httpStatus:     ${d.error?.httpStatus || "N/A"}`);
        console.log(`      message:        ${(d.error?.message || "").substring(0, 100)}`);
        console.log(`      abandonReason:  ${d.abandonReason || "N/A"}`);
        console.log("");
      });

      if (snap.size === 20) {
        console.log("  (Showing first 20. There may be more.)\n");
      }
    }
  }

  console.log("═══════════════════════════════════════════════════════════════");
  console.log("  Legend: ✅ 0 abandoned  |  🟢 <5%  |  🟡 5–30%  |  🛑 >30%");
  console.log("═══════════════════════════════════════════════════════════════\n");
}

main().catch(console.error);