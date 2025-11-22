// s3.js
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const stream = require('stream');

const region = process.env.AWS_REGION;

const s3 = new S3Client({ region });

async function uploadStreamToS3({ Bucket, Key, Body, ContentType }) {
  // Uses lib-storage's Upload which does multipart upload for streams
  const uploader = new Upload({
    client: s3,
    params: { Bucket, Key, Body, ContentType }
  });
  return uploader.done(); // resolves when done
}

async function getObjectStream({ Bucket, Key }) {
  const cmd = new GetObjectCommand({ Bucket, Key });
  const response = await s3.send(cmd);
  // response.Body is a stream-like (Node readable)
  return response.Body;
}

module.exports = { s3, uploadStreamToS3, getObjectStream };
