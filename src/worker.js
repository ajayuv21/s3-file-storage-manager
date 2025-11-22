// worker.js
const { getObjectStream } = require('./s3');
const { Job, Record } = require('./models');
const readline = require('readline');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '500', 10);

function parseLine(line, headerColumnsRef) {
  // Try JSON
  if (!line) return null;
  let trimmed = line.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed);
  } catch (e) {
    // try CSV with header heuristic
    if (headerColumnsRef.val) {
      const parts = trimmed.split(',');
      if (parts.length === headerColumnsRef.val.length) {
        const obj = {};
        headerColumnsRef.val.forEach((col, i) => obj[col] = parts[i]);
        return obj;
      }
    }
    // Could also try key:value pairs or fallback to raw line
    return { raw: line };
  }
}

async function processFile(job, queueEmitter) {
  // Update job status to running
  await Job.updateOne({ jobId: job.jobId }, { status: 'running', startedAt: new Date(), $inc: { attempts: 1 } });

  let processed = 0;
  let failed = 0;
  const headerColumnsRef = { val: null };

  const s3Stream = await getObjectStream({ Bucket: process.env.S3_BUCKET, Key: job.s3Key });

  const rl = readline.createInterface({ input: s3Stream, crlfDelay: Infinity });

  let batch = [];
  let lineNumber = 0;

  async function flushBatch() {
    if (batch.length === 0) return;
    const toInsert = batch.map(item => ({ fileId: job.fileId, data: item }));
    try {
      await Record.collection.insertMany(toInsert, { ordered: false });
      processed += batch.length;
    } catch (err) {
      // InsertMany can fail partially; count successful insert approximations
      // For simplicity, we'll treat all as attempted; in production you'd parse err.result
      failed += batch.length;
      console.error(`[Worker] insertMany error for job ${job.jobId}:`, err.message);
      // backoff on fatal DB errors
      await new Promise(res => setTimeout(res, 1000));
    } finally {
      batch = [];
      // update processed counts intermittently
      await Job.updateOne({ jobId: job.jobId }, { $inc: { processedCount: processed, failedLines: failed } });
    }
  }

  try {
    for await (const line of rl) {
      lineNumber++;
      try {
        // detect header line for CSV (if first line and contains commas and not JSON)
        if (lineNumber === 1) {
          const t = line.trim();
          if (t && t.includes(',') && !t.startsWith('{')) {
            // treat as header
            headerColumnsRef.val = t.split(',').map(s => s.trim());
            continue; // skip header line
          }
        }
        const parsed = parseLine(line, headerColumnsRef);
        if (parsed === null) continue;
        batch.push(parsed);
        if (batch.length >= BATCH_SIZE) {
          rl.pause();
          await flushBatch();
          rl.resume();
        }
      } catch (perLineErr) {
        failed++;
        console.warn(`[Worker] Job ${job.jobId} line ${lineNumber} parse error: ${perLineErr.message}`);
        // continue processing other lines
      }
    }

    // flush remaining
    await flushBatch();

    // mark succeeded
    await Job.updateOne({ jobId: job.jobId }, { status: 'succeeded', finishedAt: new Date(), processedCount: processed, failedLines: failed });
    console.log(`[Worker] Job ${job.jobId} completed; processed=${processed}, failed=${failed}`);
    queueEmitter.emit('jobSucceeded', job.jobId);
  } catch (err) {
    console.error(`[Worker] Job ${job.jobId} failed during processing:`, err);
    await Job.updateOne({ jobId: job.jobId }, { status: 'failed', error: err.message, finishedAt: new Date() });
    queueEmitter.emit('jobFailed', job.jobId, err);
  }
}

function attachWorkerToQueue(queue, concurrency) {
  // queue is JobQueue instance (EventEmitter)
  queue.on('processJob', (job) => {
    // We schedule processing in background (non-blocking)
    // processFile returns promise; worker queue manages concurrency
    processFile(job, queue);
  });
}

module.exports = { attachWorkerToQueue, processFile };
