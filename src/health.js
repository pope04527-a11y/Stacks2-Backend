// src/health.js
// Very lightweight health endpoint. Do NOT import heavy modules here.
// It checks the token in query or header and returns 200 quickly.

module.exports = function (req, res) {
  const expected = process.env.STACKL_HEALTH_TOKEN;
  const provided = req.query.token || req.header('x-health-token') || req.header('authorization');
  const token = provided && typeof provided === 'string' && provided.startsWith('Bearer ')
    ? provided.split(' ')[1]
    : provided;

  if (expected && token !== expected) {
    return res.status(401).json({ status: 'unauthorized' });
  }

  // cheap OK response (do not perform DB calls here)
  return res.status(200).json({ status: 'ok', ts: Date.now() });
};
