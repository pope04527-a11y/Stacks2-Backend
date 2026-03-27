const express = require('express');
const mongoose = require('mongoose');
const { distributeReferralCommission } = require('./commissionService'); // ensure filename matches commissionService.js

const router = express.Router();

// Admin secret - change to your real secret or supply via env var
const ADMIN_SECRET = process.env.ADMIN_SECRET || 'yoursecretpassword';

/**
 * POST /admin/process-commissions
 * Body: { adminSecret: "..." }
 *
 * Finds commissions that are not applied/processed and attempts to apply them
 * by calling the atomic distributeReferralCommission service for each commission.
 *
 * The endpoint is idempotent and safe to call multiple times.
 */
router.post('/admin/process-commissions', async (req, res) => {
  try {
    const provided = req.body && req.body.adminSecret ? req.body.adminSecret : req.headers['x-admin-secret'];
    if (!provided || provided !== ADMIN_SECRET) {
      return res.status(403).json({ success: false, message: 'Unauthorized' });
    }

    const Commission = mongoose.models.Commission || mongoose.model('Commission', new mongoose.Schema({}, { collection: 'commissions', strict: false }));

    // Find commissions that look unapplied/unprocessed
    const pending = await Commission.find({
      $or: [
        { applied: { $exists: false } },
        { applied: false },
        { processed: { $exists: false } },
        { processed: false },
        { status: { $exists: true, $ne: 'applied' } } // include items not marked applied explicitly
      ]
    }).lean();

    if (!pending || !pending.length) {
      return res.json({ success: true, message: 'No pending commissions', processed: 0 });
    }

    const applied = [];
    const failed = [];

    // Process sequentially to keep load reasonable and preserve order
    for (const c of pending) {
      try {
        if (!c || !c.sourceUserId || !c.sourceReference || typeof c.originalAmount !== 'number') {
          failed.push({ id: c && c._id, reason: 'invalid commission document' });
          continue;
        }

        // Call the atomic service to apply this commission
        const result = await distributeReferralCommission({
          sourceUserId: c.sourceUserId,
          originalAmount: c.originalAmount,
          sourceReference: c.sourceReference,
          sourceType: c.sourceType || 'task',
          note: c.note || ''
        });

        if (result && result.applied) {
          applied.push({
            id: c._id,
            recipientUserId: c.recipientUserId,
            amount: c.referralAmount,
            commissionId: result.commission && result.commission._id,
            transactionId: result.transaction && result.transaction._id
          });
        } else if (result && result.skipped) {
          // treat already_applied as success for reporting
          applied.push({ id: c._id, reason: result.reason || 'skipped' });
        } else {
          failed.push({ id: c._id, reason: JSON.stringify(result) });
        }
      } catch (err) {
        failed.push({ id: c && c._id, reason: err && err.message ? err.message : String(err) });
      }
    }

    return res.json({
      success: true,
      processedCount: applied.length,
      applied,
      failed
    });
  } catch (err) {
    console.error('Error in /admin/process-commissions', err);
    return res.status(500).json({ success: false, message: err.message || 'Internal server error' });
  }
});

module.exports = router;
