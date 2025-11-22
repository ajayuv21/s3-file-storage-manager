require('dotenv').config();
const express = require('express');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const mongoose = require('mongoose');
const JobQueue = require('./queue');
const { uploadStreamToS3 } = require('./s3');
const { File, Job } = require('./models');
const { attachWorkerToQueue } = require('./worker');

const app = express();
app.use(express.json());

// simple memory storage for multer (we will stream file from req to S3)
const upload = multer({ storage: multer.memoryStorage(), limits: { fieldSize: 25 * 1024 * 1024 } });

const PORT = process.env.PORT || 3000;
const S3_BUCKET = 'ajayuv21';
const WORKER_CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY || '2', 10);

async function start() {
 
  await mongoose.connect('mongodb+srv://dev4ajayuv:uvAjay255@cluster0.h01sc.mongodb.net/?appName=Cluster0', {});

  // initialize job queue
  const queue = new JobQueue({ concurrency: WORKER_CONCURRENCY });

  // attach worker listener
  attachWorkerToQueue(queue);

  // event handlers for logging / job state
  queue.on('enqueued', (j) => console.log(`[Queue] enqueued ${j.jobId}`));
  queue.on('start', (j) => console.log(`[Queue] starting ${j.jobId}`));
  queue.on('done', (j) => console.log(`[Queue] done ${j.jobId}`));
  queue.on('error', ({ job, err }) => console.error(`[Queue] error on job ${job.jobId}`, err));

  // Recover jobs from DB (queued/running)
  await queue.initFromDB();

  // ============ Endpoints ============

  // POST /upload : accepts multipart form-data file field named 'file'
  app.post('/upload', upload.single('file'), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'file is required (field name: file)' });

      const fileId = uuidv4();
      const originalName = req.file.originalname;
      const key = `${fileId}/${originalName}`;

      // convert buffer into stream so we still stream to S3 (for small->medium files)
      const Readable = require('stream').Readable;
      const stream = new Readable();
      stream.push(req.file.buffer);
      stream.push(null);

      await uploadStreamToS3({
        Bucket: S3_BUCKET,
        Key: key,
        Body: stream,
        ContentType: req.file.mimetype || 'text/plain'
      });

      // persist file metadata
      const fileDoc = await File.create({
        fileId,
        s3Key: key,
        originalName,
        size: req.file.size
      });

      return res.json({ fileId, s3Key: key });
    } catch (err) {
      console.error('Upload error', err);
      return res.status(500).json({ error: 'upload failed', details: err.message });
    }
  });

  // POST /process/:fileId -> create job and enqueue
  app.post('/process/:fileId', async (req, res) => {
    try {
      const { fileId } = req.params;
      const fileDoc = await File.findOne({ fileId });
      if (!fileDoc) return res.status(404).json({ error: 'file not found' });

      // create job doc
      const jobId = uuidv4();
      const job = await Job.create({
        jobId,
        fileId,
        s3Key: fileDoc.s3Key,
        status: 'queued'
      });

      // enqueue in memory
      await queue.enqueue({ jobId, fileId, s3Key: fileDoc.s3Key });

      return res.json({ jobId, status: 'queued' });
    } catch (err) {
      console.error('Enqueue error', err);
      return res.status(500).json({ error: err.message });
    }
  });

  // GET /jobs/:jobId status
  app.get('/jobs/:jobId', async (req, res) => {
    const job = await Job.findOne({ jobId: req.params.jobId }).lean();
    if (!job) return res.status(404).json({ error: 'job not found' });
    return res.json(job);
  });

  // health
  app.get('/health', (req, res) => res.send('ok'));

  // start server
  app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
  });

  // graceful shutdown
  process.on('SIGINT', async () => {
    console.log('SIGINT received. Shutting down gracefully.');
    await mongoose.connection.close();
    process.exit(0);
  });
}

start().catch(err => {
  console.error('Fatal startup error', err);
  process.exit(1);
});
