# s3-file-storage-manager


Express backend that streams uploaded text files to S3 and provides a minimal job queue to process those files into MongoDB.


## Features
- `POST /upload` — stream-upload text files to S3 (multipart, memory-efficient)
- `POST /process/:fileId` — enqueue a job that reads the file from S3, parses it line-by-line and bulk-inserts into MongoDB
- Job persistence in MongoDB so work survives restarts
- Batch inserts to avoid hammering MongoDB
- Resilient per-line parsing (bad lines don't abort the job)


## Repo layout