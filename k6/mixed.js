import http from 'k6/http';
import { check, fail } from 'k6';
import exec from 'k6/execution';
import { Counter, Gauge } from 'k6/metrics';
import {
  randomItem,
  tagWithCurrentStageIndex,
  tagWithCurrentStageProfile,
} from 'https://jslib.k6.io/k6-utils/1.6.0/index.js';
import {
  AWSConfig,
  SignatureV4,
  Endpoint,
} from 'https://jslib.k6.io/aws/0.14.0/signature.js';
import { randomBytes } from 'k6/crypto';

// ---------------------------------------------------------------- env

const S3_ENDPOINT = __ENV.S3_ENDPOINT || 'http://localhost:8000';
// Bucket pool: S3_BUCKETS (CSV) is the new contract; S3_BUCKET (single)
// is honoured for back-compat with older minirook builds and the other
// targets (rgw-native/rgw-keystone) that still set just one bucket.
const BUCKETS = (__ENV.S3_BUCKETS || __ENV.S3_BUCKET || 'k6-bench')
  .split(',')
  .map((b) => b.trim())
  .filter((b) => b.length > 0);
// Per-bucket KMS key ids for SSE=request mode (parallel to BUCKETS). The
// legacy single KMS_KEY_ID applies to every bucket when no per-bucket list
// is supplied (back-compat with the previous single-bucket script).
const _BUCKET_KMS_LIST = (__ENV.S3_BUCKET_KMS_KEYS || '').split(',').map((s) => s.trim());
const _LEGACY_KMS = __ENV.KMS_KEY_ID || '';
const BUCKET_KMS = Object.fromEntries(
  BUCKETS.map((b, i) => [b, _BUCKET_KMS_LIST[i] || _LEGACY_KMS]),
);
const OBJECT_SIZE_BYTES = parseInt(__ENV.OBJECT_SIZE_BYTES || '4096', 10);
const MIXED_WEIGHTS_RAW = __ENV.MIXED_WEIGHTS || 'get=60,put=30,delete=10';
const PRESEED_RAW = __ENV.PRESEED || '30s';
const PRESEED_LIST_MAX = parseInt(__ENV.PRESEED_LIST_MAX || '10000', 10);
const SCENARIO = __ENV.SCENARIO || '';
const CONSTANT_RATE = parseInt(__ENV.CONSTANT_RATE || '0', 10);
const CONSTANT_DURATION = __ENV.CONSTANT_DURATION || '';
// VU pool for constant-arrival-rate. preVUs = initial pool sized for ~500ms p50;
// maxVUs lets k6 scale up if p99 latency spikes, so we don't silently drop
// iterations. 10x rate tolerates per-request latency up to ~10s.
const CONSTANT_PRE_VUS = Math.max(CONSTANT_RATE * 2, 50);
const CONSTANT_MAX_VUS = Math.max(CONSTANT_RATE * 10, 500);

const awsConfig = new AWSConfig({
  region: __ENV.AWS_REGION || 'us-east-1',
  accessKeyId: __ENV.AWS_ACCESS_KEY_ID || '',
  secretAccessKey: __ENV.AWS_SECRET_ACCESS_KEY || '',
});

const signature = new SignatureV4({
  service: 's3',
  region: awsConfig.region,
  credentials: {
    accessKeyId: awsConfig.accessKeyId,
    secretAccessKey: awsConfig.secretAccessKey,
  },
  uriEscapePath: false,
  applyChecksum: true,
});

// ----------------------------------------------- weights + preseed parse

function parseWeights(raw) {
  const entries = [];
  let total = 0;
  for (const part of raw.split(',')) {
    const [op, w] = part.split('=');
    const weight = parseInt(w, 10);
    if (!['get', 'put', 'delete'].includes(op) || isNaN(weight) || weight < 0) {
      throw new Error(`Invalid MIXED_WEIGHTS entry: ${part}`);
    }
    if (weight === 0) continue;
    total += weight;
    entries.push({ op, cum: total });
  }
  if (total === 0) {
    throw new Error('MIXED_WEIGHTS must contain at least one nonzero weight');
  }
  return { entries, total };
}

function parseDurationMs(s) {
  const m = /^(\d+)(ms|s|m|h)?$/.exec(s.trim());
  if (!m) throw new Error(`Cannot parse duration: ${s}`);
  const n = parseInt(m[1], 10);
  switch (m[2] || 's') {
    case 'ms': return n;
    case 'm': return n * 60_000;
    case 'h': return n * 3_600_000;
    default: return n * 1000;
  }
}

const WEIGHTS = parseWeights(MIXED_WEIGHTS_RAW);

let PRESEED_MODE;
let PRESEED_MS = 0;
{
  const v = PRESEED_RAW.trim().toLowerCase();
  if (v === 'none' || v === '0' || v === '0s') {
    PRESEED_MODE = 'none';
  } else if (v === 'existing') {
    PRESEED_MODE = 'existing';
  } else {
    PRESEED_MODE = 'duration';
    PRESEED_MS = parseDurationMs(PRESEED_RAW);
  }
}

const body = randomBytes(OBJECT_SIZE_BYTES);

// k6 evaluates this module per VU, so module-level `myKeys` is VU-local.
let myKeys = [];
let initialised = false;

// -------------------------------------------------------- metrics

const httpServerErrors = new Counter('http_server_errors');
const httpCriticalErrors = new Counter('http_critical_errors');
const httpSaturationErrors = new Counter('http_saturation_errors');
const httpClientErrors = new Counter('http_client_errors');
const mixedSkipped = new Counter('mixed_skipped');
const putObjectSize = new Gauge('put_object_size');

function bandError(status) {
  if (status === 503) {
    // RGW returns 503 for both rate-limit (SlowDown) and throttler/scheduler
    // shedding (Beast rgw_max_concurrent_requests, dmclock). Both = backpressure.
    // RGW never emits 429 — a fronting proxy might, but we hit the svc directly.
    httpSaturationErrors.add(1, { status: String(status) });
  } else if (status >= 500 && status < 600) {
    // 500 InternalError = real bug / unmapped RADOS error.
    // 502/504 = proxy/network (shouldn't happen with direct svc connection).
    httpServerErrors.add(1, { status: String(status) });
  } else if (status === 403) {
    // S3-spec compliance: AccessDenied, SignatureDoesNotMatch, InvalidAccessKeyId,
    // expired Keystone token, Barbican KMS lookup failure. Also QuotaExceeded —
    // technically saturation, but indistinguishable without parsing the <Code>
    // element. We don't set bucket/user quotas here, so 403 ≈ misconfig.
    httpCriticalErrors.add(1, { status: String(status) });
  } else if (status >= 400 && status < 500) {
    httpClientErrors.add(1, { status: String(status) });
  }
}

// -------------------------------------------------------- signing / S3

function sseHeaders(bucket) {
  const key = BUCKET_KMS[bucket];
  if (!key) return {};
  return {
    'x-amz-server-side-encryption': 'aws:kms',
    'x-amz-server-side-encryption-aws-kms-key-id': key,
  };
}

function pickBucket() {
  return BUCKETS[Math.floor(Math.random() * BUCKETS.length)];
}

function signedReq(method, path, headers, query, payload) {
  return signature.sign(
    {
      method,
      endpoint: new Endpoint(S3_ENDPOINT),
      path,
      headers: headers || {},
      query: query || {},
      body: payload || null,
    },
    {},
  );
}

function doRequest(method, path, extraHeaders, query, payload, tags) {
  // Only sign Content-Length when we actually send a body. For GET/DELETE
  // k6's http.request strips Content-Length when payload is null, so signing
  // it produces a SigV4 mismatch (RGW: "Secret string does not correctly
  // sign payload") even though PUT works.
  const headers = { ...(extraHeaders || {}) };
  if (payload) {
    headers['Content-Length'] = String(payload.byteLength);
  }
  const signed = signedReq(method, path, headers, query, payload);
  return http.request(method, signed.url, payload, {
    headers: signed.headers,
    tags: tags || {},
  });
}

function doPut(bucket, key, phase) {
  const res = doRequest(
    'PUT', `/${bucket}/${key}`, sseHeaders(bucket), {}, body,
    { op: 'put', phase, bucket },
  );
  check(res, { 'put 2xx': (r) => r.status >= 200 && r.status < 300 }, { op: 'put', phase });
  bandError(res.status);
  return res;
}

function doGet(bucket, key, phase) {
  const res = doRequest(
    'GET', `/${bucket}/${key}`, {}, {}, null,
    { op: 'get', phase, bucket },
  );
  // 404 on GET is tolerated under PRESEED=existing + DELETE weight (another
  // VU may have deleted the key between pickOp() and the request).
  check(res, {
    'get 2xx/404': (r) => (r.status >= 200 && r.status < 300) || r.status === 404,
  }, { op: 'get', phase });
  if (res.status !== 404) bandError(res.status);
  return res;
}

function doDelete(bucket, key, phase) {
  const res = doRequest(
    'DELETE', `/${bucket}/${key}`, {}, {}, null,
    { op: 'delete', phase, bucket },
  );
  // 404 on DELETE is tolerated (e.g. concurrent VUs).
  check(res, {
    'delete 2xx/404': (r) => (r.status >= 200 && r.status < 300) || r.status === 404,
  }, { op: 'delete', phase });
  if (res.status !== 404) bandError(res.status);
  return res;
}

function pickOp() {
  const r = Math.random() * WEIGHTS.total;
  for (const e of WEIGHTS.entries) {
    if (r < e.cum) return e.op;
  }
  return WEIGHTS.entries[WEIGHTS.entries.length - 1].op;
}

// Paginated bucket listing for PRESEED=existing across the BUCKETS pool.
// Returns [{bucket, key}, ...], capped at `max` total entries.
// NOTE: keys containing XML entities (&, <, >) come back entity-encoded;
// this helper does not decode them. Safe for our seed/live/* key shapes
// (alphanumeric + '/'). If pointed at a bucket populated by another tool
// with such keys, GET/DELETE on the affected keys will 404.
function listAllKeys(max) {
  const found = [];
  for (const bucket of BUCKETS) {
    if (found.length >= max) break;
    let marker = '';
    while (found.length < max) {
      const query = { 'max-keys': '1000' };
      if (marker) query.marker = marker;
      const signed = signedReq('GET', `/${bucket}/`, {}, query, null);
      const res = http.request('GET', signed.url, null, {
        headers: signed.headers,
        responseType: 'text',
      });
      if (res.status !== 200) {
        throw new Error(`LIST ${bucket} failed: ${res.status} ${res.body}`);
      }
      const keyRe = /<Key>([^<]+)<\/Key>/g;
      let m;
      let pageCount = 0;
      let lastKey = '';
      while ((m = keyRe.exec(res.body)) !== null) {
        lastKey = m[1];
        found.push({ bucket, key: lastKey });
        pageCount++;
        if (found.length >= max) break;
      }
      const truncated = /<IsTruncated>true<\/IsTruncated>/.test(res.body);
      if (!truncated || pageCount === 0 || found.length >= max) break;
      marker = lastKey;
    }
  }
  return found;
}

// ---------------------------------------------------- ramping scenarios

const _scenarioDefs = {
  testing: {
    stages: [{ duration: '1m', target: 2342 }],
    preAllocatedVUs: 2342,
  },
  stress_minikube: {
    // Sized for a 4 CPU / 16 GB minikube VM on a Mac. The RGW pod, OSDs,
    // mons, mgr, monitoring stack, keystone, barbican, and the k6 runner
    // all share that 4-core budget — so peak is intentionally modest.
    // ~5min total, four plateaus, 1min hold each to read steady-state p95.
    stages: [
      { duration: '20s', target: 50 },  { duration: '1m', target: 50 },
      { duration: '20s', target: 150 }, { duration: '1m', target: 150 },
      { duration: '20s', target: 300 }, { duration: '1m', target: 300 },
      { duration: '20s', target: 500 }, { duration: '1m', target: 500 },
    ],
    preAllocatedVUs: 1000,
  },
  demo: {
    stages: [
      { duration: '30s', target: 150 }, { duration: '2m', target: 150 }, // light
      { duration: '30s', target: 300 }, { duration: '2m', target: 300 }, // knee
      { duration: '30s', target: 400 }, { duration: '2m', target: 400 }, // saturated
    ],
    preAllocatedVUs: 2000,
  },
  breakpoint: {
    stages: [
      { duration: '5m', target: 1000 },
    ],
    preAllocatedVUs: 1000,
  },
  stress_workstation: {
    stages: [
      { duration: '30s', target: 100 },  { duration: '3m', target: 100 },
      { duration: '30s', target: 250 },  { duration: '3m', target: 250 },
      { duration: '30s', target: 500 },  { duration: '3m', target: 500 },
      { duration: '30s', target: 750 },  { duration: '3m', target: 750 },
      { duration: '30s', target: 1000 }, { duration: '3m', target: 1000 },
      { duration: '30s', target: 3000 }, { duration: '1m', target: 3000 },
      { duration: '30s', target: 4000 }, { duration: '1m', target: 4000 },
      { duration: '30s', target: 5000 }, { duration: '1m', target: 5000 },
      { duration: '30s', target: 6000 }, { duration: '1m', target: 6000 },
      { duration: '30s', target: 7000 }, { duration: '1m', target: 7000 },
      { duration: '30s', target: 8000 }, { duration: '1m', target: 8000 },
      { duration: '30s', target: 9000 }, { duration: '1m', target: 9000 },
    ],
    preAllocatedVUs: 9000,
  },
  stress_cloud: {
    stages: [
      { duration: '30s', target: 5000 },   { duration: '5m', target: 5000 },
      { duration: '30s', target: 10000 },  { duration: '5m', target: 10000 },
      { duration: '30s', target: 25000 },  { duration: '5m', target: 25000 },
      { duration: '30s', target: 50000 },  { duration: '5m', target: 50000 },
      { duration: '30s', target: 75000 },  { duration: '5m', target: 75000 },
      { duration: '30s', target: 100000 }, { duration: '5m', target: 100000 },
      { duration: '30s', target: 125000 }, { duration: '5m', target: 125000 },
      { duration: '30s', target: 150000 }, { duration: '5m', target: 150000 },
    ],
    preAllocatedVUs: 150000,
  },
};

// --------------------------------------------------------- options

const baseOptions = {
  discardResponseBodies: true,
  batch: 100,
  batchPerHost: 100,
  setupTimeout: '600m',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
  summaryTimeUnit: 'ms',
  systemTags: ['status', 'method', 'error_code', 'ip'],
  thresholds: {
    http_req_failed: ['rate<0.10'],
    checks: ['rate>0.95'],
    http_client_errors: ['rate<0.05'],
    http_saturation_errors: ['rate<0.20'],   // backpressure is expected at the limit
    http_server_errors: ['rate<0.01'],       // real 500s should be near-zero
    // Critical = auth/misconfig; abort immediately so a misconfigured run
    // doesn't burn 30 minutes hammering 403s. 10s grace window absorbs
    // transient signing/clock skew at startup.
    http_critical_errors: [
      { threshold: 'count < 1', abortOnFail: true, delayAbortEval: '10s' },
    ],
    // Loud signal when k6 can't keep up with the requested arrival rate
    // (preAllocated/maxVUs starved). Only meaningful for *-arrival-rate executors.
    dropped_iterations: ['count<10'],
  },
};

export const options = (() => {
  if (!SCENARIO) return baseOptions;
  if (SCENARIO === 'constant') {
    if (CONSTANT_RATE <= 0) {
      throw new Error('SCENARIO=constant requires CONSTANT_RATE > 0');
    }
    if (!CONSTANT_DURATION) {
      throw new Error('SCENARIO=constant requires CONSTANT_DURATION');
    }
    return {
      ...baseOptions,
      scenarios: {
        load_test: {
          executor: 'constant-arrival-rate',
          rate: CONSTANT_RATE,
          timeUnit: '1s',
          duration: CONSTANT_DURATION,
          preAllocatedVUs: CONSTANT_PRE_VUS,
          maxVUs: CONSTANT_MAX_VUS,
        },
      },
    };
  }
  const def = _scenarioDefs[SCENARIO];
  if (!def) {
    const names = ['constant', ...Object.keys(_scenarioDefs)].join(', ');
    throw new Error(`Unknown SCENARIO '${SCENARIO}'; choose one of ${names}`);
  }
  return {
    ...baseOptions,
    scenarios: {
      load_test: {
        executor: 'ramping-arrival-rate',
        timeUnit: '1s',
        preAllocatedVUs: def.preAllocatedVUs,
        stages: def.stages,
      },
    },
  };
})();

// --------------------------------------------------------- setup

export function setup() {
  const ssePerBucket = Object.values(BUCKET_KMS).filter((v) => v).length;
  console.log(
    `mixed.js setup: endpoint=${S3_ENDPOINT} buckets=${BUCKETS.length} ` +
    `(${BUCKETS.join(',')}) sse_keys=${ssePerBucket}/${BUCKETS.length} ` +
    `weights=${MIXED_WEIGHTS_RAW} preseed=${PRESEED_RAW} ` +
    `(mode=${PRESEED_MODE}, ms=${PRESEED_MS}) ` +
    `obj_size=${OBJECT_SIZE_BYTES} scenario=${SCENARIO || '(none)'}` +
    (SCENARIO === 'constant'
      ? ` rate=${CONSTANT_RATE}/s for ${CONSTANT_DURATION}`
        + ` (preVUs=${CONSTANT_PRE_VUS}, maxVUs=${CONSTANT_MAX_VUS})`
      : ''),
  );

  // Idempotent createBucket for every bucket. minirook.py also creates them
  // up-front, but this self-heals when the script is run standalone.
  for (const bucket of BUCKETS) {
    const signed = signedReq('PUT', `/${bucket}/`, { 'Content-Length': '0' }, {}, null);
    const res = http.request('PUT', signed.url, null, {
      headers: signed.headers,
      responseType: 'text',
    });
    if (res.status !== 200 && res.status !== 409) {
      fail(`createBucket ${bucket} failed: ${res.status} ${res.body}`);
    }
  }
  putObjectSize.add(OBJECT_SIZE_BYTES);

  if (PRESEED_MODE === 'existing') {
    console.log(
      `PRESEED=existing: listing keys across ${BUCKETS.length} bucket(s) ` +
      `(cap=${PRESEED_LIST_MAX})...`,
    );
    const keys = listAllKeys(PRESEED_LIST_MAX);
    console.log(`PRESEED=existing: discovered ${keys.length} keys`);
    if (keys.length === 0) {
      console.warn('PRESEED=existing but bucket(s) empty; GET/DELETE ops will be skipped.');
    }
    return { mode: 'existing', keys, testStartTime: Date.now(), preseedMs: 0 };
  }

  if (PRESEED_MODE === 'none') {
    const needsKeys = WEIGHTS.entries.some((e) => e.op === 'get' || e.op === 'delete');
    if (needsKeys) {
      console.warn(
        'PRESEED=none with GET/DELETE in weights — those ops will be skipped until PUTs populate keys.',
      );
    }
  }

  return { mode: PRESEED_MODE, keys: [], testStartTime: Date.now(), preseedMs: PRESEED_MS };
}

// --------------------------------------------------------- default

export default function (data) {
  if (!initialised) {
    if (data.mode === 'existing' && data.keys && data.keys.length > 0) {
      myKeys = data.keys.slice();
    }
    initialised = true;
  }

  // Stage helpers only work with stage-based executors (ramping-*). The
  // `constant` scenario uses constant-arrival-rate, which has no stages.
  if (SCENARIO && SCENARIO !== 'constant') {
    tagWithCurrentStageIndex();
    tagWithCurrentStageProfile();
  }

  const phase =
    data.mode === 'duration' && (Date.now() - data.testStartTime) < data.preseedMs
      ? 'preseed'
      : 'mixed';

  if (phase === 'preseed') {
    const bucket = pickBucket();
    const key = `seed/${exec.vu.idInTest}/${exec.scenario.iterationInTest}`;
    const res = doPut(bucket, key, phase);
    if (res.status >= 200 && res.status < 300) {
      myKeys.push({ bucket, key });
    }
    return;
  }

  const op = pickOp();
  if (op === 'put') {
    const bucket = pickBucket();
    const key = `live/${exec.vu.idInTest}/${exec.scenario.iterationInTest}`;
    const res = doPut(bucket, key, phase);
    if (res.status >= 200 && res.status < 300) {
      myKeys.push({ bucket, key });
    }
  } else if (op === 'get') {
    if (myKeys.length === 0) {
      mixedSkipped.add(1, { op, reason: 'no_keys' });
      return;
    }
    const pick = randomItem(myKeys);
    doGet(pick.bucket, pick.key, phase);
  } else if (op === 'delete') {
    if (myKeys.length === 0) {
      mixedSkipped.add(1, { op, reason: 'no_keys' });
      return;
    }
    const idx = Math.floor(Math.random() * myKeys.length);
    const pick = myKeys[idx];
    const res = doDelete(pick.bucket, pick.key, phase);
    if ((res.status >= 200 && res.status < 300) || res.status === 404) {
      myKeys.splice(idx, 1);
    }
  }
}

// --------------------------------------------------------- summary
//
// minirook.py parses the MINIROOK_VERDICT line out of the streamed pod logs
// and uses it to distinguish "finished cleanly" from "finished with failed
// thresholds" — the TestRun phase alone doesn't differentiate the two.

export function handleSummary(data) {
  const failed = [];
  for (const [name, m] of Object.entries(data.metrics || {})) {
    for (const [expr, t] of Object.entries(m.thresholds || {})) {
      if (!t.ok) failed.push({ metric: name, threshold: expr });
    }
  }
  const verdict = { ok: failed.length === 0, failed };
  return {
    stdout: `\n===MINIROOK_VERDICT===\n${JSON.stringify(verdict)}\n===END_MINIROOK_VERDICT===\n`,
  };
}
