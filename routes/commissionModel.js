const mongoose = require('mongoose');

const CommissionSchema = new mongoose.Schema({
  recipientUserId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  recipientInviteCode: { type: String, required: true },
  sourceUserId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  sourceType: { type: String, enum: ['task', 'sale', 'training', 'other'], default: 'task' },

  originalAmount: { type: Number, required: true },
  referralAmount: { type: Number, required: true },

  // Unique source reference (used for idempotency)
  sourceReference: { type: String, required: true, unique: true },

  note: { type: String, default: '' },

  // Processing / auditing fields
  status: { type: String, enum: ['pending', 'applied', 'cancelled'], default: 'pending', index: true },
  processed: { type: Boolean, default: false },
  processedAt: { type: Date, default: null },
  applied: { type: Boolean, default: false },
  appliedAt: { type: Date, default: null },
  appliedBy: { type: String, default: '' },
  transactionId: { type: mongoose.Schema.Types.ObjectId, ref: 'Transaction', default: null },
  inviterBalanceAfter: { type: Number, default: null }

}, {
  timestamps: true,
  collection: 'commissions'
});

// Optional compound indexes for faster reconciliation queries
CommissionSchema.index({ processed: 1, status: 1 });
CommissionSchema.index({ recipientUserId: 1, processed: 1 });

module.exports = mongoose.models.Commission || mongoose.model('Commission', CommissionSchema);
