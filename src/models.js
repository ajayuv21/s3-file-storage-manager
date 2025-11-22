const mongoose = require('mongoose');

const FileSchema = new mongoose.Schema({
  fileId: { type: String, unique: true, index: true },
  s3Key: String,
  originalName: String,
  size: Number,
  uploadedAt: { type: Date, default: Date.now }
});

const JobSchema = new mongoose.Schema({
  jobId: { type: String, unique: true, index: true },
  fileId: String,
  s3Key: String,
  status: { type: String, enum: ['queued','running','succeeded','failed','cancelled'], default: 'queued' },
  attempts: { type: Number, default: 0 },
  error: String,
  createdAt: { type: Date, default: Date.now },
  startedAt: Date,
  finishedAt: Date,
  processedCount: { type: Number, default: 0 },
  failedLines: { type: Number, default: 0 }
});

const RecordSchema = new mongoose.Schema({
  fileId: String,
  data: mongoose.Schema.Types.Mixed
}, { strict: false });

const File = mongoose.model('File', FileSchema);
const Job = mongoose.model('Job', JobSchema);
const Record = mongoose.model('Record', RecordSchema);

module.exports = { File, Job, Record };
