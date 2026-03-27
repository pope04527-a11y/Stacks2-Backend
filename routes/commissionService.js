/**
 * commissionService.js
 *
 * Atomic, idempotent distribution of referral commissions.
 *
 * Usage:
 *   const { distributeReferralCommission } = require('./commissionService');
 *   await distributeReferralCommission({ sourceUserId, originalAmount, sourceReference, sourceType, note });
 *
 * Notes:
 * - Expects commissionModel.js to exist (Commission).
 * - Will try to require ./transactionModel; if not present it falls back to a minimal dynamic Transaction model.
 * - Uses the registered User model from your app (api.js registers the User schema).
 */

const mongoose = require('mongoose');
const Commission = require('./commissionModel'); // existing file in repo

// Try to require an explicit Transaction model file if present, otherwise use a dynamic fallback
let Transaction;
try {
  Transaction = require('./transactionModel');
} catch (e) {
  Transaction = mongoose.models.Transaction || mongoose.model('Transaction', new mongoose.Schema({}, { collection: 'transactions', strict: false }));
}

const REFERRAL_RATE = 0.20; // 20% referral rate - adjust if needed

function yyyyMmDd(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

/**
 * Distribute referral commission for a training account.
 * - Idempotent: checks commission by sourceReference to avoid double application.
 * - Transactional: attempts atomic operation with session.withTransaction; falls back to best-effort if transactions are unavailable.
 *
 * Params:
 *  - sourceUserId: ObjectId (training account)  [required]
 *  - originalAmount: Number (commission base amount)  [required]
 *  - sourceReference: String unique per task (e.g. `task:<taskId>:completed`)  [required]
 *  - sourceType: String e.g. 'task'  [optional]
 *  - note: String  [optional]
 *
 * Returns:
 *  - { skipped: true, reason }  OR
 *  - { applied: true, commission, transaction, updatedUser }
 */
async function distributeReferralCommission({ sourceUserId, originalAmount, sourceReference, sourceType = 'task', note = '' }) {
  if (!mongoose.Types.ObjectId.isValid(sourceUserId)) throw new Error('Invalid sourceUserId');
  if (typeof originalAmount !== 'number' || originalAmount <= 0) throw new Error('originalAmount must be a positive number');
  if (!sourceReference || typeof sourceReference !== 'string') throw new Error('sourceReference required');

  const session = await mongoose.startSession();

  // Non-transaction fallback (best-effort)
  async function fallbackNoTransaction() {
    const UserModel = mongoose.model('User');
    const trainingUser = await UserModel.findById(sourceUserId);
    if (!trainingUser) return { skipped: true, reason: 'training user not found (fallback)' };

    const inviterCode = trainingUser.referredBy;
    if (!inviterCode) return { skipped: true, reason: 'no referredBy (fallback)' };

    const inviter = await UserModel.findOne({ $or: [{ inviteCode: inviterCode }, { invite_code: inviterCode }] });
    if (!inviter) return { skipped: true, reason: 'inviter not found (fallback)', inviteCode: inviterCode };

    // Idempotency check
    const existing = await Commission.findOne({ sourceReference });
    if (existing) {
      if (existing.applied || existing.status === 'applied' || existing.processed) {
        return { skipped: true, reason: 'already_applied' };
      }
      // otherwise we will try to apply the existing commission
    }

    const referralAmount = Math.round((originalAmount * REFERRAL_RATE + Number.EPSILON) * 100) / 100;

    // Create commission doc if it doesn't exist
    let commissionDoc = existing;
    if (!existing) {
      commissionDoc = await Commission.create({
        recipientUserId: inviter._id,
        recipientInviteCode: inviter.inviteCode || inviter.invite_code || '',
        sourceUserId: trainingUser._id,
        sourceType,
        originalAmount,
        referralAmount,
        sourceReference,
        note,
        processed: false,
        status: 'pending'
      });
    }

    // Create a transaction ledger entry
    const tx = await Transaction.create({
      userId: inviter._id,
      username: inviter.username,
      amount: referralAmount,
      type: 'commission',
      direction: 'credit',
      status: 'completed',
      reference: `commission:${commissionDoc._id}`,
      metadata: { commissionId: commissionDoc._id, source: sourceReference },
      createdAt: new Date()
    });

    // Update inviter user: commission (total), balance, commissionToday (with reset logic)
    const today = yyyyMmDd();
    const lastReset = inviter.lastCommissionReset ? yyyyMmDd(new Date(inviter.lastCommissionReset)) : null;

    const userUpdate = { $inc: { commission: referralAmount, balance: referralAmount } };
    if (lastReset !== today) {
      userUpdate.$set = { commissionToday: referralAmount, lastCommissionReset: today };
    } else {
      userUpdate.$inc = { ...(userUpdate.$inc || {}), commissionToday: referralAmount };
    }

    const updatedUser = await mongoose.model('User').findByIdAndUpdate(inviter._id, userUpdate, { new: true });

    // Mark commission applied
    await Commission.updateOne({ _id: commissionDoc._id }, {
      $set: {
        applied: true,
        appliedAt: new Date(),
        appliedBy: 'fallback',
        processed: true,
        processedAt: new Date(),
        status: 'applied',
        transactionId: tx._id,
        inviterBalanceAfter: (updatedUser && updatedUser.balance) || (inviter.balance || 0) + referralAmount,
        updatedAt: new Date()
      }
    });

    return { applied: true, commission: commissionDoc, transaction: tx, updatedUser };
  }

  try {
    let result = null;

    await session.withTransaction(async () => {
      const UserModel = mongoose.model('User');
      const trainingUser = await UserModel.findById(sourceUserId).session(session);
      if (!trainingUser) { result = { skipped: true, reason: 'training user not found' }; return; }

      const inviterCode = trainingUser.referredBy;
      if (!inviterCode) { result = { skipped: true, reason: 'no referredBy' }; return; }

      const inviter = await UserModel.findOne({ $or: [{ inviteCode: inviterCode }, { invite_code: inviterCode }] }).session(session);
      if (!inviter) { result = { skipped: true, reason: 'inviter not found', inviteCode: inviterCode }; return; }

      // Idempotency: check existing commission by sourceReference
      const existing = await Commission.findOne({ sourceReference }).session(session);
      if (existing) {
        if (existing.applied || existing.status === 'applied' || existing.processed) {
          result = { skipped: true, reason: 'already_applied', commission: existing };
          return;
        }
        // else we'll apply the existing commission (do not recreate)
      }

      const referralAmount = Math.round((originalAmount * REFERRAL_RATE + Number.EPSILON) * 100) / 100;

      // Create commission doc if missing
      let createdCommission = existing;
      if (!existing) {
        const created = await Commission.create([{
          recipientUserId: inviter._id,
          recipientInviteCode: inviter.inviteCode || inviter.invite_code || '',
          sourceUserId: trainingUser._id,
          sourceType,
          originalAmount,
          referralAmount,
          sourceReference,
          note,
          processed: false,
          status: 'pending'
        }], { session });
        createdCommission = created[0];
      }

      // Create transaction record (ledger)
      const txDoc = {
        userId: inviter._id,
        username: inviter.username,
        amount: referralAmount,
        type: 'commission',
        direction: 'credit',
        status: 'completed',
        reference: `commission:${createdCommission._id}`,
        metadata: { commissionId: createdCommission._id, source: sourceReference },
        createdAt: new Date()
      };
      const [createdTx] = await Transaction.create([txDoc], { session });

      // Update inviter user: commission total, balance, commissionToday
      const today = yyyyMmDd();
      const lastReset = inviter.lastCommissionReset ? yyyyMmDd(new Date(inviter.lastCommissionReset)) : null;

      const userUpdate = { $inc: { commission: referralAmount, balance: referralAmount } };
      if (lastReset !== today) {
        userUpdate.$set = { commissionToday: referralAmount, lastCommissionReset: today };
      } else {
        userUpdate.$inc = { ...(userUpdate.$inc || {}), commissionToday: referralAmount };
      }

      const updatedUser = await UserModel.findByIdAndUpdate(inviter._id, userUpdate, { new: true, session });
      if (!updatedUser) throw new Error('Failed to update inviter user');

      // Mark commission as applied inside transaction
      await Commission.updateOne({ _id: createdCommission._id }, {
        $set: {
          applied: true,
          appliedAt: new Date(),
          appliedBy: 'system',
          processed: true,
          processedAt: new Date(),
          status: 'applied',
          transactionId: createdTx._id,
          inviterBalanceAfter: updatedUser.balance,
          updatedAt: new Date()
        }
      }).session(session);

      result = { applied: true, commission: createdCommission, transaction: createdTx, updatedUser };
    });

    return result;
  } catch (err) {
    // If transaction path fails, attempt fallback best-effort
    try {
      const fb = await fallbackNoTransaction();
      return fb;
    } catch (fbErr) {
      throw fbErr;
    }
  } finally {
    session.endSession();
  }
}

module.exports = { distributeReferralCommission };
