import test from 'node:test';
import assert from 'node:assert/strict';
import handler, { greeting } from './api/index.mjs';
test('greeting trims input, defaults blank names, and escapes markup', () => {
  assert.equal(greeting(' Nick '), 'Hello, Nick!');
  assert.equal(greeting('   '), 'Hello, exe.dev!');
  assert.equal(greeting('<script>'), 'Hello, &lt;script&gt;!');
});
test('health route returns structured provider status', () => {
  const res = {
    setHeader(k, v) {
      this[k] = v;
    },
    end(body) {
      this.body = body;
    },
  };
  handler({ url: '/health' }, res);
  assert.deepEqual(JSON.parse(res.body), { ok: true, provider: 'exe.dev' });
  assert.equal(res['content-type'], 'application/json');
});
