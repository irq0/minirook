import http from 'k6/http';
import { check, fail } from 'k6';
import { options as baseOptions } from './options.js';

export const options = baseOptions;

const keystoneUrl = __ENV.KEYSTONE_URL;
const barbicanUrl = __ENV.BARBICAN_URL;
const username = __ENV.OS_USERNAME || 'admin';
const password = __ENV.OS_PASSWORD || 'password';
const project = __ENV.OS_PROJECT || 'admin';
const domain = __ENV.OS_DOMAIN || 'Default';
const kmsKey = __ENV.KMS_KEY_ID || '';

export function setup() {
  const resp = http.post(
    `${keystoneUrl}/v3/auth/tokens`,
    JSON.stringify({
      auth: {
        identity: {
          methods: ['password'],
          password: {
            user: {
              name: username,
              password,
              domain: { name: domain },
            },
          },
        },
        scope: { project: { name: project, domain: { name: domain } } },
      },
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (resp.status !== 201) {
    fail(`Keystone token request failed: ${resp.status} ${resp.body}`);
  }
  const token = resp.headers['X-Subject-Token'] || resp.headers['x-subject-token'];
  if (!token) {
    fail('Keystone response missing X-Subject-Token header');
  }
  return { token };
}

export default function (data) {
  const headers = { 'X-Auth-Token': data.token };

  if (kmsKey) {
    const resp = http.get(`${barbicanUrl}/v1/secrets/${kmsKey}`, { headers });
    check(resp, { 'barbican secret read 200': (r) => r.status === 200 });
    const payloadResp = http.get(`${barbicanUrl}/v1/secrets/${kmsKey}/payload`, {
      headers: { ...headers, Accept: 'application/octet-stream' },
    });
    check(payloadResp, { 'barbican payload read 200': (r) => r.status === 200 });
  } else {
    const resp = http.get(`${barbicanUrl}/v1/secrets?limit=10`, { headers });
    check(resp, { 'barbican list secrets 200': (r) => r.status === 200 });
  }
}
