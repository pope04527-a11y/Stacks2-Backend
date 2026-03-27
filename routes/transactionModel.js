const mongoose = require('mongoose');

const TransactionSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  username: { type: String, default: '' },

  amount: { type: Number, required: true },
  type: { type: String, default: 'commission' }, // commission|deposit|withdraw|admin_add_balance etc
  direction: { type: String, enum: ['credit', 'debit'], default: 'credit' },
  status: { type: String, default: 'completed' }, // pending|completed|failed etc

  reference: { type: String, default: '' }, // e.g. "commission:<commissionId>"
  metadata: { type: mongoose.Schema.Types.Mixed, default: {} },

  createdAt: { type: Date, default: () => new Date() }
}, {
  collection: 'transactions',
  strict: false
});

// Helpful indexes for queries / reporting
TransactionSchema.index({ userId: 1, createdAt: -1 });
TransactionSchema.index({ reference: 1 });

module.exports = mongoose.models.Transaction || mongoose.model('Transaction', TransactionSchema);
