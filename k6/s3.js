import { check, fail } from 'k6';
import exec from 'k6/execution';
import {
  AWSConfig,
  S3Client,
} from 'https://jslib.k6.io/aws/0.14.0/s3.js';
import { options as baseOptions } from './options.js';

export const options = baseOptions;

const cfg = new AWSConfig({
  region: __ENV.AWS_REGION || 'us-east-1',
  accessKeyId: __ENV.AWS_ACCESS_KEY_ID,
  secretAccessKey: __ENV.AWS_SECRET_ACCESS_KEY,
  endpoint: __ENV.S3_ENDPOINT,
  forcePathStyle: true,
});

const s3 = new S3Client(cfg);
const bucket = __ENV.S3_BUCKET || 'k6-bench';
const objectSize = parseInt(__ENV.OBJECT_SIZE_BYTES || '4096', 10);
const kmsKeyId = __ENV.KMS_KEY_ID || '';
const body = 'x'.repeat(objectSize);

export function setup() {
  try {
    s3.createBucket(bucket);
  } catch (e) {
    // Tolerate BucketAlreadyOwnedByYou / BucketAlreadyExists on rerun.
  }
}

export default function () {
  const key = `vu-${exec.vu.idInTest}/iter-${exec.scenario.iterationInTest}`;

  const putParams = {};
  if (kmsKeyId) {
    putParams.serverSideEncryption = 'aws:kms';
    putParams.ssekmsKeyId = kmsKeyId;
  }

  try {
    s3.putObject(bucket, key, body, putParams);
  } catch (e) {
    fail(`putObject failed: ${e}`);
  }

  let got;
  try {
    got = s3.getObject(bucket, key);
  } catch (e) {
    fail(`getObject failed: ${e}`);
  }

  check(got, {
    'roundtrip size matches': (o) => o && o.data && o.data.length === body.length,
  });

  try {
    s3.deleteObject(bucket, key);
  } catch (e) {
    // Best-effort cleanup; don't fail the iteration.
  }
}
