// queue.js
const EventEmitter = require('events');
const { Job } = require('./models');
const os = require('os');

class JobQueue extends EventEmitter {
  constructor({ concurrency = 2 } = {}) {
    super();
    this.concurrency = concurrency;
    this.running = 0;
    this.queue = []; // in-memory queue of job objects { jobId, fileId, s3Key }
    this.processingSet = new Set();
  }

  async initFromDB() {
    // re-enqueue jobs that are queued or running (recovery)
    const pending = await Job.find({ status: { $in: ['queued', 'running'] } }).sort({ createdAt: 1 }).lean();
    pending.forEach(j => this.enqueueFromDb(j));
    console.log(`[Queue] Loaded ${pending.length} pending jobs from DB`);
    this.processNext();
  }

  enqueueFromDb(jobDoc) {
    const job = { jobId: jobDoc.jobId, fileId: jobDoc.fileId, s3Key: jobDoc.s3Key };
    this.queue.push(job);
  }

  async enqueue(jobObj) {
    // jobObj should contain jobId, fileId, s3Key
    this.queue.push(jobObj);
    this.emit('enqueued', jobObj);
    this.processNext();
  }

  async processNext() {
    while (this.running < this.concurrency && this.queue.length > 0) {
      const job = this.queue.shift();
      if (this.processingSet.has(job.jobId)) continue;
      this.processingSet.add(job.jobId);
      this.running += 1;
      this._process(job).finally(() => {
        this.running -= 1;
        this.processingSet.delete(job.jobId);
        // loop to start more
        setImmediate(() => this.processNext());
      });
    }
  }

  async _process(job) {
    try {
      this.emit('start', job);
      await this._callWorker(job);
      this.emit('done', job);
    } catch (err) {
      this.emit('error', { job, err });
    }
  }

  async _callWorker(job) {
    // This delegates actual work to listeners (worker.js attaches listener)
    return new Promise((resolve, reject) => {
      // one-time listeners for this job
      const onSuccess = (jid) => { if (jid === job.jobId) { cleanup(); resolve(); } };
      const onFail = (jid, err) => { if (jid === job.jobId) { cleanup(); reject(err); } };
      const cleanup = () => {
        this.removeListener('jobSucceeded', onSuccess);
        this.removeListener('jobFailed', onFail);
      };
      this.on('jobSucceeded', onSuccess);
      this.on('jobFailed', onFail);
      // trigger processing (worker must listen to 'processJob' event)
      this.emit('processJob', job);
    });
  }
}

module.exports = JobQueue;
