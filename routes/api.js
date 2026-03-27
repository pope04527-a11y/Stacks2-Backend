const express = require('express');
const crypto = require('crypto');
const mongoose = require('mongoose');
const cloudinary = require('cloudinary').v2;
const axios = require('axios');
const { URLSearchParams } = require('url');

const { distributeReferralCommission } = require('./commissionService');

const router = express.Router();

// ========== MODELS WITH EXPLICIT SCHEMA ==========
// Added tokenIssuedAt and tokenInvalidBefore to support server-side invalidation (per-user)
// Keep strict: false for compatibility with existing documents
const userSchema = new mongoose.Schema({
  username: String,
  phone: String,
  loginPassword: String,
  withdrawPassword: String,
  walletAddress: String,
  exchange: String,
  gender: String,
  balance: { type: Number, default: 0 },
  commission: { type: Number, default: 0 },
  commissionToday: { type: Number, default: 0 },
  lastCommissionReset: { type: String, default: "" }, // <-- Added for midnight reset tracking
  vipLevel: { type: Number, default: 1 },
  inviteCode: String,
  referredBy: String,
  token: { type: String, default: "" },
  tokenIssuedAt: { type: Number, default: null }, // timestamp (ms) when token was issued
  tokenInvalidBefore: { type: Number, default: 0 }, // invalidate tokens issued before this per-user timestamp
  suspended: { type: Boolean, default: false },
  currentSet: { type: Number, default: 1 },
  // store starting balance for current set so we can enforce min-product-price rule
  setStartingBalance: { type: Number, default: null },
  createdAt: String,

  // New fields for credit score & admin flag
  creditScore: { type: Number, default: 100 }, // 0 - 100 scale, default 100%
  isAdmin: { type: Boolean, default: false },

}, { collection: 'users', strict: false });

const User = mongoose.models.User || mongoose.model('User', userSchema);
const Task = mongoose.models.Task || mongoose.model('Task', new mongoose.Schema({}, { collection: 'tasks', strict: false }));
const Combo = mongoose.models.Combo || mongoose.model('Combo', new mongoose.Schema({}, { collection: 'combos', strict: false }));
const Log = mongoose.models.Log || mongoose.model('Log', new mongoose.Schema({}, { collection: 'logs', strict: false }));
const Deposit = mongoose.models.Deposit || mongoose.model('Deposit', new mongoose.Schema({}, { collection: 'deposits', strict: false }));
const Withdrawal = mongoose.models.Withdrawal || mongoose.model('Withdrawal', new mongoose.Schema({}, { collection: 'withdrawals', strict: false }));
const Notification = mongoose.models.Notification || mongoose.model('Notification', new mongoose.Schema({}, { collection: 'notifications', strict: false }));
const Transaction = mongoose.models.Transaction || mongoose.model('Transaction', new mongoose.Schema({}, { collection: 'transactions', strict: false }));
const LinkClick = mongoose.models.LinkClick || mongoose.model('LinkClick', new mongoose.Schema({}, { collection: 'linkclicks', strict: false }));
const Setting = mongoose.models.Setting || mongoose.model('Setting', new mongoose.Schema({}, { collection: 'settings', strict: false }));

// Create helpful indexes (best-effort at startup). This speeds token/user lookups and the task count query.
(async () => {
  try {
    // user token lookup
    if (User && User.collection) {
      await User.collection.createIndex({ token: 1 }, { background: true }).catch(() => {});
      await User.collection.createIndex({ username: 1 }, { background: true }).catch(() => {});
    }
    // task count: username + status + set
    if (Task && Task.collection) {
      await Task.collection.createIndex({ username: 1, status: 1, set: 1 }, { background: true }).catch(() => {});
    }
    console.log('DB indexes ensured: users(token, username), tasks(username,status,set)');
  } catch (err) {
    console.warn('Index creation warning:', err && err.message ? err.message : err);
  }
})();

cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME || 'dycytqdfj',
    api_key: process.env.CLOUDINARY_API_KEY || '983286743251596',
    api_secret: process.env.CLOUDINARY_API_SECRET || 'zeU4nedVzVzvqqndh2MF82AdRiI',
    secure: true
});

// ========== Product cache & helpers (pre-warm + in-flight dedupe + periodic refresh) ==========
const CLOUDINARY_CACHE_DURATION = 1000 * 60 * 5; // 5 minutes
let cachedProducts = [];
let lastCloudinaryFetch = 0;
let cloudinaryFetchInFlight = null; // promise for dedupe

// Helper: extract price using multiple heuristics (context, tags, public_id)
function extractPriceFromResource(r) {
  if (!r) return undefined;

  // 1) context.custom.price preferred
  if (r.context && r.context.custom && typeof r.context.custom.price !== 'undefined' && r.context.custom.price !== 'N/A') {
    const p = parseFloat(String(r.context.custom.price).replace(/[^\d.]/g, ''));
    if (!Number.isNaN(p)) return p;
  }

  // 2) tags like price_123 or price-123
  if (Array.isArray(r.tags) && r.tags.length) {
    for (const t of r.tags) {
      if (!t) continue;
      const mTag = String(t).match(/^price[_-]?(\d+(?:\.\d+)?)$/i);
      if (mTag) {
        const p = parseFloat(mTag[1]);
        if (!Number.isNaN(p)) return p;
      }
    }
  }

  // 3) trailing-numeric-tokens heuristic on public_id
  if (typeof r.public_id === 'string') {
    const parts = r.public_id.split('_').filter(Boolean);
    const trailing = [];
    for (let i = parts.length - 1; i >= 0; i--) {
      const t = parts[i];
      if (/^\d+$/.test(t)) {
        trailing.unshift(t);
        if (trailing.length >= 3) break;
      } else break;
    }
    if (trailing.length >= 2) {
      const intPart = trailing[trailing.length - 2];
      const fracPart = trailing[trailing.length - 1];
      const cand = parseFloat(`${intPart}.${fracPart}`);
      if (!Number.isNaN(cand)) return cand;
    } else if (trailing.length === 1) {
      const cand = parseFloat(trailing[0]);
      if (!Number.isNaN(cand)) return cand;
    }

    // fallback: longest numeric token anywhere
    const tokens = (r.public_id.match(/\d+/g) || []).map(s => s.replace(/^0+/, '') || '0');
    if (tokens.length) {
      tokens.sort((a, b) => {
        if (b.length !== a.length) return b.length - a.length;
        return Number(b) - Number(a);
      });
      const cand = parseFloat(tokens[0]);
      if (!Number.isNaN(cand)) return cand;
    }
  }

  return undefined;
}

// Helper: generate a random price between min and max (two decimals)
function generateRandomPrice(min = 10, max = 100) {
  const lo = Number(min) || 10;
  const hi = Number(max) || 100;
  const v = Math.random() * (hi - lo) + lo;
  return Math.round((v + Number.EPSILON) * 100) / 100;
}

async function fetchProductsFromCloudinary() {
  if (cloudinaryFetchInFlight) return cloudinaryFetchInFlight;

  cloudinaryFetchInFlight = (async () => {
    const prefixEnv = (process.env.CLOUDINARY_PRODUCTS_PREFIX || 'products/').toString();
    let products = [];
    let next_cursor = undefined;

    try {
      do {
        const opts = {
          type: 'upload',
          max_results: 500,
          context: true,
          tags: true,
          ...(next_cursor ? { next_cursor } : {})
        };
        if (prefixEnv) opts.prefix = prefixEnv;

        const result = await cloudinary.api.resources(opts);
        const pageProducts = (result.resources || []).map(r => {
          const name = (r.context && r.context.custom && (r.context.custom.caption || r.context.custom.name))
            || r.filename
            || (typeof r.public_id === 'string' ? r.public_id.split('/').pop() : r.public_id);

          const description = (r.context && r.context.custom && (r.context.custom.alt || r.context.custom.description)) || '';

          const price = extractPriceFromResource(r);
          const finalPrice = (typeof price === 'number' && !Number.isNaN(price)) ? Number(price) : undefined;

          return {
            image: r.secure_url,
            name,
            price: finalPrice,
            description,
            public_id: r.public_id
          };
        }).filter(p => p.image); // only keep items with url

        products = products.concat(pageProducts);
        next_cursor = result.next_cursor;
      } while (next_cursor);

      // Assign reasonable random prices to items lacking a numeric price
      const numericPrices = products.map(p => p.price).filter(v => typeof v === 'number' && !Number.isNaN(v));
      let median = 25;
      if (numericPrices.length) {
        numericPrices.sort((a, b) => a - b);
        median = numericPrices[Math.floor(numericPrices.length / 2)];
      }
      const randMin = Math.max(1, median * 0.5);
      const randMax = Math.max(randMin + 1, median * 1.5);

      let randomAssignedCount = 0;
      products = products.map(p => {
        if (typeof p.price !== 'number' || Number.isNaN(p.price)) {
          randomAssignedCount++;
          return { ...p, price: generateRandomPrice(randMin, randMax), _priceAssigned: 'random' };
        }
        return { ...p, _priceAssigned: 'extracted' };
      });

      cachedProducts = products;
      lastCloudinaryFetch = Date.now();

      console.log(`Cloudinary fetch: loaded ${cachedProducts.length} product(s) (prefix='${prefixEnv}'). numeric:${numericPrices.length}; randomAssigned:${randomAssignedCount}`);

      return cachedProducts;
    } finally {
      cloudinaryFetchInFlight = null;
    }
  })();

  return cloudinaryFetchInFlight;
}

/**
 * Returns cached products. If cache is empty it waits for initial fetch (caller will wait).
 * If cache is stale but non-empty, returns cached and triggers background refresh.
 */
async function getCachedCloudinaryProducts() {
  const now = Date.now();
  if (cachedProducts.length && (now - lastCloudinaryFetch < CLOUDINARY_CACHE_DURATION)) {
    return cachedProducts;
  }
  if (!cachedProducts.length) {
    try {
      return await fetchProductsFromCloudinary();
    } catch (err) {
      console.warn('Cloudinary initial fetch failed:', err && err.message ? err.message : err);
      return cachedProducts || [];
    }
  }
  // stale but present -> refresh in background
  fetchProductsFromCloudinary().catch(err => {
    console.warn('Cloudinary background refresh failed:', err && err.message ? err.message : err);
  });
  return cachedProducts;
}

/**
 * Waits up to timeoutMs for an initial fetch; if timeout triggers returns cachedProducts (may be empty).
 * Useful to avoid blocking start-task too long if Cloudinary is temporarily slow.
 */
async function getCachedCloudinaryProductsWithTimeout(timeoutMs = 800) {
  const now = Date.now();
  if (cachedProducts.length && (now - lastCloudinaryFetch < CLOUDINARY_CACHE_DURATION)) {
    return cachedProducts;
  }
  try {
    const fetchPromise = getCachedCloudinaryProducts();
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('cloudinary_timeout')), timeoutMs));
    return await Promise.race([fetchPromise, timeout]);
  } catch (err) {
    // on timeout or error return whatever cached we have (maybe empty)
    return cachedProducts || [];
  }
}

// Pre-warm cache on startup (best-effort, non-blocking)
setImmediate(() => {
  fetchProductsFromCloudinary()
    .then(() => console.log('Cloudinary cache pre-warmed, items:', cachedProducts.length))
    .catch(err => console.warn('Cloudinary pre-warm failed:', err && err.message ? err.message : err));
});

// Periodic refresh
setInterval(() => {
  fetchProductsFromCloudinary().catch(err => {
    console.warn('Periodic Cloudinary refresh failed:', err && err.message ? err.message : err);
  });
}, CLOUDINARY_CACHE_DURATION);

// ========== Utility & config ==========
const MIN_STARTING_CAPITAL_PERCENT = 0.30; // 30%

function generateInviteCode() {
    const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    const digits = '0123456789';
    let letterCount = Math.random() < 0.5 ? 2 : 3;
    let digitCount = 6 - letterCount;
    let codeArr = [];
    for (let i = 0; i < letterCount; i++) {
        codeArr.push(letters.charAt(Math.floor(Math.random() * letters.length)));
    }
    for (let i = 0; i < digitCount; i++) {
        codeArr.push(digits.charAt(Math.floor(Math.random() * digits.length)));
    }
    for (let i = codeArr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [codeArr[i], codeArr[j]] = [codeArr[j], codeArr[i]];
    }
    return codeArr.join('');
}

const vipRules = {
    1: { tasks: 40, commissionRate: 0.005, combinedProfit: 0.03, activation: 100, setsPerDay: 3 },
    2: { tasks: 45, commissionRate: 0.01, combinedProfit: 0.06, activation: 500, setsPerDay: 3 },
    3: { tasks: 50, commissionRate: 0.015, combinedProfit: 0.09, activation: 2000, setsPerDay: 3 },
    4: { tasks: 55, commissionRate: 0.02, combinedProfit: 0.12, activation: 5000, setsPerDay: 3 }
};

function hasPendingComboTask(tasks, user) {
    return tasks.some(t =>
        t.username === user.username &&
        t.isCombo &&
        (t.status === 'Pending' || t.status === 'pending')
    );
}

function hasPendingTask(tasks, user) {
    return tasks.some(t =>
        t.username === user.username &&
        !t.isCombo &&
        (t.status === 'Pending' || t.status === 'pending')
    );
}

// ========== Platform status helpers & middleware (NEW) ==========
function getUKHour() {
  try {
    const parts = new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Europe/London',
      hour: '2-digit',
      hour12: false
    }).formatToParts(new Date());
    const hourPart = parts.find(p => p.type === 'hour');
    return parseInt(hourPart ? hourPart.value : new Date().toLocaleString('en-GB', { timeZone: 'Europe/London', hour12: false }).split(':')[0], 10);
  } catch (err) {
    return new Date().getUTCHours();
  }
}

async function getOrCreateSettings() {
  let settings = await Setting.findOne({});
  if (!settings) {
    settings = await Setting.create({
      platformClosed: false,
      autoOpenHourUK: 10,
      whoCanAccessDuringClose: [],
      service: { whatsapp: "", telegram: "" },
      globalTokenInvalidBefore: 0
    });
  } else {
    const updates = {};
    if (typeof settings.platformClosed === 'undefined') updates.platformClosed = false;
    if (typeof settings.autoOpenHourUK === 'undefined') updates.autoOpenHourUK = 10;
    if (!Array.isArray(settings.whoCanAccessDuringClose)) updates.whoCanAccessDuringClose = [];
    if (!settings.service) updates.service = { whatsapp: "", telegram: "" };
    if (typeof settings.globalTokenInvalidBefore === 'undefined') updates.globalTokenInvalidBefore = 0;
    if (Object.keys(updates).length) {
      await Setting.updateOne({ _id: settings._id }, { $set: updates });
      settings = await Setting.findById(settings._id);
    }
  }
  return settings;
}

async function checkPlatformStatus(req, res, next) {
  try {
    const settings = await getOrCreateSettings();

    const ukHour = getUKHour();

    // Auto-open if hour is >= configured hour and platform currently closed
    if (settings.platformClosed && typeof settings.autoOpenHourUK === 'number' && !isNaN(settings.autoOpenHourUK)) {
      if (ukHour >= Number(settings.autoOpenHourUK)) {
        settings.platformClosed = false;
        await settings.save();
      }
    }

    // If still closed, check allowlist (normalize username + allowlist entries)
    if (settings.platformClosed) {
      const usernameRaw = req.user && req.user.username ? req.user.username : null;
      const username = usernameRaw ? usernameRaw.trim().toLowerCase() : null;

      if (!username || !Array.isArray(settings.whoCanAccessDuringClose) || !settings.whoCanAccessDuringClose.includes(username)) {
        return res.json({ success: false, message: "The system is temporarily closed. Tasks and withdrawals are disabled at the moment. Please try again later." });
      }
    }

    next();
  } catch (err) {
    console.error('checkPlatformStatus middleware error:', err && err.message ? err.message : err);
    next();
  }
}

// ========== Auth middleware (enhanced to support global/per-user invalidation) ==========
const verifyUserToken = async (req, res, next) => {
    const token = req.headers['x-auth-token'] || req.headers['X-Auth-Token'] || (req.headers.authorization ? (req.headers.authorization.startsWith('Bearer ') ? req.headers.authorization.slice(7) : null) : null);
    if (!token) {
        return res.status(403).json({ success: false, message: 'Missing authentication token' });
    }
    // find user by token (existing approach)
    const user = await User.findOne({ token });
    if (!user) return res.status(403).json({ success: false, message: 'Invalid or expired token' });

    try {
      // check per-user and global invalidation timestamps
      const settings = await getOrCreateSettings();
      const globalInvalidBefore = Number(settings.globalTokenInvalidBefore || 0);
      const userInvalidBefore = Number(user.tokenInvalidBefore || 0);
      const issuedAt = Number(user.tokenIssuedAt || 0);
      if (issuedAt && (issuedAt < globalInvalidBefore || issuedAt < userInvalidBefore)) {
        // token was issued before invalidation threshold
        // clear token on server side (best-effort) to prevent further use
        try {
          await User.updateOne({ _id: user._id }, { $set: { token: "", tokenIssuedAt: null } });
        } catch (e) {}
        return res.status(403).json({ success: false, message: 'Invalid or expired token' });
      }
    } catch (err) {
      console.error('verifyUserToken validation error:', err && err.message ? err.message : err);
      // fall through to allow or deny? safer to deny if uncertain
    }

    req.user = user;
    next();
};

// ========== Endpoints ==========

// Settings
// --- Replace the existing router.get('/settings', ...) handler with this block ---
router.get('/settings', async (req, res) => {
  try {
    const settings = await getOrCreateSettings();
    // convert to plain object (lean representation) if it's a Mongoose doc
    const sObj = settings && typeof settings.toObject === 'function' ? settings.toObject() : (settings || {});

    // Normalize legacy withdrawFee -> withdrawFeePercent for clients
    let withdrawFeePercent = typeof sObj.withdrawFeePercent !== 'undefined'
      ? sObj.withdrawFeePercent
      : (typeof sObj.withdrawFee !== 'undefined' ? sObj.withdrawFee : 0);
    withdrawFeePercent = withdrawFeePercent || 0;

    // Platform closing aliases for compatibility with frontend
    const autoOpenHour = (typeof sObj.autoOpenHourUK === 'number') ? sObj.autoOpenHourUK : 10;
    const hh = String(autoOpenHour).padStart(2, '0');
    const autoOpenTime = `${hh}:00`;

    const allowList = Array.isArray(sObj.whoCanAccessDuringClose) ? sObj.whoCanAccessDuringClose : [];

    // Prevent intermediate CDNs / browsers from serving stale cached version
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');

    // Compose response: include all stored settings fields (including `currency`) and also the compatibility fields above
    const response = {
      ...sObj,
      // override/ensure these computed keys are present for backward compatibility
      withdrawFeePercent,
      autoOpenHourUK: autoOpenHour,
      autoOpenTime,
      whoCanAccessDuringClose: allowList,
      allowList
    };

    // Optionally remove internal mongo fields before returning
    if (response._id) delete response._id;
    if (response.__v) delete response.__v;

    return res.json(response);
  } catch (err) {
    console.error('GET /api/settings error:', err);
    return res.status(500).json({ success: false, message: 'Failed to load settings' });
  }
});
// --- end replacement block ---

// Registration
router.post('/users/register', async (req, res) => {
    const {
        username,
        phone,
        loginPassword,
        withdrawalPassword,
        gender,
        inviteCode
    } = req.body;

    if (!username || !loginPassword || !withdrawalPassword || !phone || !inviteCode) {
        return res.status(400).json({ success: false, message: "All fields (username, phone, loginPassword, withdrawalPassword, inviteCode) are required." });
    }

    const usernameExists = await User.findOne({ username });
    if (usernameExists) {
        return res.json({ success: false, message: "Username already exists." });
    }

    const phoneExists = await User.findOne({ phone });
    if (phoneExists) {
        return res.json({ success: false, message: "Phone already registered." });
    }

    const referrer = await User.findOne({ $or: [{ inviteCode: inviteCode.trim() }, { invite_code: inviteCode.trim() }] });
    if (!referrer) {
        return res.json({ success: false, message: "Invalid invitation code. Please provide a valid code from an existing user." });
    }

    let userInviteCode, unique = false, tries = 0;
    while (!unique && tries < 1000) {
        userInviteCode = generateInviteCode();
        const exists = await User.findOne({ $or: [{ inviteCode: userInviteCode }, { invite_code: userInviteCode }] });
        if (!exists) unique = true;
        tries++;
    }
    if (!unique) {
        return res.status(500).json({ success: false, message: "Failed to generate unique invitation code." });
    }

    const newUser = {
        username: username.trim(),
        phone: phone.trim(),
        loginPassword: loginPassword.trim(),
        withdrawPassword: withdrawalPassword.trim(),
        gender: gender || "Male",
        inviteCode: userInviteCode,
        referredBy: inviteCode.trim(),
        vipLevel: 1,
        balance: 0,
        commission: 0,
        commissionToday: 0,
        lastCommissionReset: "", // <-- Added here for new users
        taskCountToday: 0,
        suspended: false,
        token: crypto.randomBytes(24).toString('hex'),
        tokenIssuedAt: Date.now(),
        createdAt: new Date().toISOString(),
        currentSet: 1,
        // ensure new users start at full credit
        creditScore: 100
    };

    await User.create(newUser);

    return res.json({ success: true, user: newUser });
});

// Authentication (login) — trigger async cache pre-warm so subsequent start-task is fast
router.post('/login', async (req, res) => {
    const input = req.body.input || req.body.username || "";
    const password = req.body.password;
    const user = await User.findOne({
        $or: [{ username: input }, { phone: input }],
        loginPassword: password
    });
    if (user) {
        if (user.suspended) return res.status(403).json({ success: false, message: 'Account suspended' });

        // Issue a new token and record issued time to support global/per-user invalidation
        user.token = crypto.randomBytes(24).toString('hex');
        user.tokenIssuedAt = Date.now();
        await user.save();

        // pre-warm product cache (non-blocking)
        fetchProductsFromCloudinary().catch(err => {
          console.warn('Cloudinary pre-warm after login failed:', err && err.message ? err.message : err);
        });

        return res.json({ success: true, user });
    }
    res.status(401).json({ success: false, message: 'Invalid credentials' });
});

// Wallet bind
router.post('/bind-wallet', verifyUserToken, async (req, res) => {
    const { fullName, exchange, walletAddress } = req.body;
    const user = req.user;
    if (!exchange || !walletAddress) {
        return res.json({ success: false, message: "Exchange and wallet address required" });
    }
    if (fullName) user.fullName = fullName;
    user.exchange = exchange;
    user.walletAddress = walletAddress;
    await user.save();
    res.json({ success: true });
});

// User profile
router.get('/user-profile', verifyUserToken, async (req, res) => {
    try {
        // Force no caching by clients and intermediate caches so profile reflects latest state on each navigation
        res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0');
        res.set('Pragma', 'no-cache');
        res.set('Expires', '0');
        res.set('Surrogate-Control', 'no-store');

        // Lean + projection to avoid creating a full Mongoose document when only a few fields are needed
        const dbUser = await User.findOne({ username: req.user.username })
          .select('username balance vipLevel commissionToday currentSet inviteCode referredBy exchange walletAddress fullName lastCommissionReset creditScore isAdmin setStartingBalance')
          .lean();

        if (!dbUser) return res.status(404).json({ success: false, message: "User not found" });

        // Ensure currentSet is numeric
        const userSet = (typeof dbUser.currentSet === 'number' && !isNaN(dbUser.currentSet)) ? dbUser.currentSet : 1;

        // Midnight commission reset safety (perform efficient update rather than saving whole doc)
        const todayStr = new Date().toISOString().slice(0, 10);
        if (dbUser.lastCommissionReset !== todayStr) {
            try {
                await User.updateOne(
                    { username: dbUser.username },
                    { $set: { commissionToday: 0, lastCommissionReset: todayStr } }
                );
                dbUser.commissionToday = 0;
                dbUser.lastCommissionReset = todayStr;
            } catch (e) {
                // best-effort: log and continue
                console.warn('Failed to update commission reset for user', dbUser.username, e && e.message ? e.message : e);
            }
        }

        // Instead of loading all tasks and filtering in-memory (very slow on large collections),
        // count completed tasks for this user and set using an indexed query.
        // Use case-insensitive match for status == "completed"
        const completedStatusRegex = { $regex: /^completed$/i };
        let taskCountThisSet = 0;
        try {
            taskCountThisSet = await Task.countDocuments({
                username: dbUser.username,
                set: userSet,
                status: completedStatusRegex
            });
        } catch (e) {
            console.warn('Task count failed for user', dbUser.username, e && e.message ? e.message : e);
            taskCountThisSet = 0;
        }

        const vipInfo = vipRules[dbUser.vipLevel] || vipRules[1];

        return res.json({
            success: true,
            user: {
                id: dbUser._id,
                username: dbUser.username,
                balance: dbUser.balance ?? 0,
                vipLevel: dbUser.vipLevel ?? 1,
                commissionToday: dbUser.commissionToday ?? 0,
                taskCountThisSet,
                currentSet: userSet,
                maxTasks: vipInfo.tasks,
                inviteCode: dbUser.inviteCode ?? "",
                referredBy: dbUser.referredBy ?? "",
                exchange: dbUser.exchange ?? "",
                walletAddress: dbUser.walletAddress ?? "",
                fullName: dbUser.fullName ?? "",
                creditScore: (typeof dbUser.creditScore !== 'undefined') ? dbUser.creditScore : 100,
                isAdmin: !!dbUser.isAdmin
            }
        });
    } catch (err) {
        console.error('GET /api/user-profile error:', err && err.message ? err.message : err);
        return res.status(500).json({ success: false, message: 'Failed to load profile' });
    }
});

// Product recommendation
router.get('/recommend-product', verifyUserToken, async (req, res) => {
    const user = req.user;
    try {
        const products = await getCachedCloudinaryProducts();

        let affordable = products.filter(prod => prod.price <= user.balance);
        if (!affordable.length) affordable = products;
        if (!affordable.length) {
            return res.json({ success: false, message: "No products available for your balance." });
        }
        const chosenProduct = affordable[Math.floor(Math.random() * affordable.length)];

        const vipInfo = vipRules[user.vipLevel] || vipRules[1];
        const commission = Math.floor(chosenProduct.price * vipInfo.commissionRate * 100) / 100;

        res.json({
            success: true,
            product: {
                ...chosenProduct,
                commission
            }
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Failed to fetch products', error: err.message });
    }
});

// Task records
router.get('/task-records', verifyUserToken, async (req, res) => {
    const tasks = await Task.find({ username: req.user.username });
    const user = req.user;
    let records = [];
    tasks.forEach(t => {
        if (t.isCombo && Array.isArray(t.products)) {
            if (t.status === 'Pending' || t.status === 'pending') {
                if (t.products.length === 2) {
                    records.push({
                        ...t.toObject(),
                        comboIndex: 0,
                        canSubmit: false,
                        status: 'Pending',
                        product: t.products[0]
                    });
                    records.push({
                        ...t.toObject(),
                        comboIndex: 1,
                        canSubmit: true,
                        status: 'Pending',
                        product: t.products[1]
                    });
                } else {
                    t.products.forEach((prod, idx) => {
                        records.push({
                            ...t.toObject(),
                            comboIndex: idx,
                            canSubmit: idx === t.products.length - 1,
                            status: 'Pending',
                            product: prod
                        });
                    });
                }
            } else {
                t.products.forEach((prod, idx) => {
                    records.push({
                        ...t.toObject(),
                        comboIndex: idx,
                        canSubmit: false,
                        status: 'Completed',
                        product: prod
                    });
                });
            }
        } else {
            records.push({
                ...t.toObject(),
                canSubmit: true
            });
        }
    });
    records.sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
    res.json({ success: true, records });
});

// Start task (with 30% starting-capital enforcement)
// Middleware checkPlatformStatus applied here to block when platformClosed.
router.post('/start-task', verifyUserToken, checkPlatformStatus, async (req, res) => {
    try {
        // Re-fetch fresh user doc to get up-to-date balance and setStartingBalance
        let user = await User.findById(req.user._id);
        if (!user) return res.status(401).json({ success: false, message: 'Unauthorized' });
        if (typeof user.toObject === 'function') user = user.toObject();

        if (typeof user.currentSet !== "number") user.currentSet = 1;
        const userSet = user.currentSet || 1;

        // === Robust task counting: fetch tasks only for the current set (treat legacy docs without set as set 1) ===
        let taskQuery;
        if (userSet === 1) {
          // include tasks with set === 1 OR missing set (legacy)
          taskQuery = { username: user.username, $or: [{ set: 1 }, { set: { $exists: false } }] };
        } else {
          taskQuery = { username: user.username, set: userSet };
        }
        const tasks = await Task.find(taskQuery).lean();

        // fetch combos for the user (unchanged)
        const combos = await Combo.find({ username: user.username }).lean();

        // counts
        const tasksStarted = tasks.length;
        const tasksCompleted = tasks.filter(t => (t.status || '').toLowerCase() === 'completed').length;

        // DEBUG: helpful log to diagnose off-by-one issues (remove or reduce level in prod)
        console.log('start-task debug', {
          username: user.username,
          userSet,
          tasksStarted,
          tasksCompleted,
          tasksPreview: tasks.map(t => ({ _id: t._id, set: t.set, status: t.status, startedAt: t.startedAt || t.createdAt })),
          comboTriggers: combos.map(c => ({ _id: c._id, trigger: c.triggerTaskNumber }))
        });

        // use the filtered tasks (same set) for pending-checks
        if (hasPendingComboTask(tasks || [], user)) {
            return res.json({ success: false, message: "You must submit all combo products before starting new tasks." });
        }
        if (hasPendingTask(tasks || [], user)) {
            return res.json({ success: false, message: "You must submit your current product before starting another." });
        }

        if (tasksStarted === 0 && user.balance < 50) {
            return res.json({ success: false, message: 'You need at least £50 balance to start your first task set.' });
        }
        const vipInfo = vipRules[user.vipLevel] || vipRules[1];
        const maxTasks = vipInfo.tasks;
        if (tasksStarted >= maxTasks) {
            return res.json({ success: false, message: 'You have completed your current set. Please ask admin to reset your account for the next set.' });
        }

        // Determine setStartingBalance: record current balance when first task in a set is started
        let setStartingBalance = user.setStartingBalance;
        if (tasksStarted === 0) {
          setStartingBalance = Number(user.balance || 0);
          await User.updateOne({ _id: user._id }, { $set: { setStartingBalance } });
        }
        setStartingBalance = Number(setStartingBalance || user.balance || 0);

        // compute minimum allowed price (30% of setStartingBalance)
        const minAllowedPrice = Math.round((setStartingBalance * MIN_STARTING_CAPITAL_PERCENT + Number.EPSILON) * 100) / 100;

        // fetch products (fast cached getter)
        const products = await getCachedCloudinaryProducts();

        // Filter products: enforce both affordability and minAllowedPrice
        let affordable = (products || []).filter(p => p && typeof p.price === 'number' && p.price <= user.balance && p.price >= minAllowedPrice);

        // If none found within user's current balance, relax to all cached products but still enforce minAllowedPrice
        if (!affordable.length) {
          affordable = (products || []).filter(p => p && typeof p.price === 'number' && p.price >= minAllowedPrice);
        }

        if (!affordable.length) {
            return res.status(400).json({ success: false, message: `No products available matching the starting-capital rule. Minimum product price must be at least ${minAllowedPrice.toFixed(2)} GBP (30% of your set starting capital).` });
        }

        const chosenProduct = affordable[Math.floor(Math.random() * affordable.length)];

        // Combo logic (for combos enforce that comboTotal >= minAllowedPrice)
        let comboToTrigger = null;

        /*
          Semantics decision:
          - The code originally triggered when Number(combo.triggerTaskNumber) === (tasksStarted + 1)
            (i.e. when the user is starting the Nth task).
          - To avoid off-by-one issues caused by tasks from other sets or missing set fields,
            we now count tasks strictly for the current set (above). We now match combos by
            the number of completed tasks (tasksCompleted), so a combo with trigger=14 will be
            considered after the user has 14 completed tasks (and the next start will create the combo task).
        */

        comboToTrigger = combos.find(combo =>
            Number(combo.triggerTaskNumber) === tasksCompleted && combo.username === user.username
        );

        if (comboToTrigger && comboToTrigger.products && comboToTrigger.products.length === 2) {
            const comboTotal = comboToTrigger.products.reduce((sum, prod) => sum + Number(prod.price || 0), 0);

            if (comboTotal < minAllowedPrice) {
              return res.status(400).json({ success: false, message: `Combo total (${comboTotal.toFixed(2)} GBP) does not meet the minimum starting-capital rule (${minAllowedPrice.toFixed(2)} GBP).` });
            }

            await User.updateOne(
                { _id: user._id },
                { $inc: { balance: -comboTotal } }
            );

            const taskCode = crypto.randomBytes(10).toString('hex');
            const now = new Date().toISOString();

            const comboTask = {
                username: user.username,
                products: comboToTrigger.products.map(prod => ({
                    ...prod,
                    image: prod.image && typeof prod.image === 'string' && prod.image.trim() !== '' && prod.image !== 'null'
                        ? prod.image
                        : chosenProduct.image,
                    status: 'Pending',
                    submitted: false,
                    createdAt: now,
                    code: crypto.randomBytes(6).toString('hex')
                })),
                status: 'Pending',
                startedAt: now,
                taskCode,
                set: userSet,
                isCombo: true
            };

            await Task.create(comboTask);

            const updatedUser = await User.findById(user._id);
            const isNegative = updatedUser.balance < 0;

            return res.json({
                success: true,
                task: comboTask,
                isCombo: true,
                comboMustSubmitAllAtOnce: true,
                currentBalance: updatedUser.balance,
                isNegativeBalance: isNegative
            });
        }

        // Single task flow
        if (user.balance < chosenProduct.price) {
            return res.json({ success: false, message: 'Insufficient balance for recommended product.' });
        }
        const commission = Math.floor(chosenProduct.price * vipInfo.commissionRate * 100) / 100;

        await User.updateOne(
            { _id: user._id },
            { $inc: { balance: -chosenProduct.price } }
        );

        const taskCode = crypto.randomBytes(10).toString('hex');

        const task = {
            username: user.username,
            product: {
                name: chosenProduct.name,
                price: chosenProduct.price,
                commission,
                image: chosenProduct.image,
                createdAt: new Date().toISOString(),
                code: crypto.randomBytes(6).toString('hex'),
                public_id: chosenProduct.public_id,
                description: chosenProduct.description
            },
            status: 'Pending',
            startedAt: new Date().toISOString(),
            taskCode,
            set: userSet
        };

        await Task.create(task);

        res.json({ success: true, task });
    } catch (err) {
        console.error('start-task error:', err);
        res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    }
});

// ----------------------- Optimized submit-task: target < 1.5s -----------------------
// Key optimizations:
// - Use lean() when reading the task
// - Perform User and Task updates in parallel (Promise.all)
// - Do not await distributeReferralCommission; fire-and-forget it so response returns fast
// - Build response object locally to avoid an extra DB read
// Middleware checkPlatformStatus applied here to block when platformClosed.
router.post('/submit-task', verifyUserToken, checkPlatformStatus, async (req, res) => {
    const { taskCode } = req.body;
    const user = req.user;

    try {
      // Read task in lean mode (fast)
      const task = await Task.findOne({ taskCode, username: user.username }).lean();
      if (!task) return res.status(404).json({ success: false, message: 'Task not found' });

      // Combo tasks
      if (task.isCombo && Array.isArray(task.products)) {
        if (user.balance < 0) {
          return res.json({ success: false, mustDeposit: true, message: "Insufficient balance. Please deposit to clear negative balance before submitting combo products." });
        }

        const now = new Date().toISOString();
        const updatedProducts = task.products.map(prod => ({ ...prod, status: 'Completed', submitted: true, completedAt: now }));

        const totalRefund = updatedProducts.reduce((sum, prod) => sum + Number(prod.price || 0), 0);
        const totalCommission = updatedProducts.reduce((sum, prod) => sum + Number(prod.commission || 0), 0);

        // Parallel updates: user balance and task status
        const userUpdatePromise = User.updateOne(
          { _id: user._id },
          { $inc: { balance: totalRefund + totalCommission, commission: totalCommission, commissionToday: totalCommission } }
        );
        const taskUpdatePromise = Task.updateOne(
          { _id: task._id },
          { $set: { products: updatedProducts, status: 'Completed', completedAt: now } }
        );

        await Promise.all([userUpdatePromise, taskUpdatePromise]);

        // Fire-and-forget referral distribution so we return quickly (<1.5s)
        (async () => {
          try {
            const sourceRef = `task:${task._id}:completed`;
            await distributeReferralCommission({
              sourceUserId: user._id,
              originalAmount: totalCommission,
              sourceReference: sourceRef,
              sourceType: 'task',
              note: `Referral from combo task ${task._id}`
            });
          } catch (err) {
            console.error('Referral distribution failed (combo, async):', err);
          }
        })();

        // After marking completed, check if the set is now complete
        try {
          const taskSet = task.set || 1;
          const vipInfo = vipRules[user.vipLevel] || vipRules[1];
          const completedCount = await Task.countDocuments({ username: user.username, set: taskSet, status: { $regex: /^completed$/i } });
          if (completedCount >= (vipInfo.tasks || 40)) {
            const todayKey = getUKDateKey();
            // Atomically increment registeredWorkingDays[todayKey], and mark that reset is requested.
            // IMPORTANT: do NOT auto-increment currentSet anymore.
            const updates = {
              $inc: { [`registeredWorkingDays.${todayKey}`]: 1 },
              $set: { setStartingBalance: null, resetRequested: true }
            };
            await User.updateOne({ _id: user._id }, updates);
            // do NOT increment currentSet automatically here
          }
        } catch (err) {
          console.error('post-combo-completion bookkeeping failed:', err);
        }

        // Construct response without doing another DB read
        const responseTask = {
          ...task,
          products: updatedProducts,
          status: 'Completed',
          completedAt: now
        };

        return res.json({ success: true, task: responseTask });
      }

      // Normal task flow
      if (task.status?.toLowerCase() !== 'pending') {
        return res.status(404).json({ success: false, message: 'Task already submitted or not pending' });
      }

      const vipInfo = vipRules[user.vipLevel] || vipRules[1];
      const price = Number(task.product.price);
      const commission = Math.floor(price * vipInfo.commissionRate * 100) / 100;
      const now = new Date().toISOString();

      // Parallel updates: user and task (fast)
      const userUpdatePromise = User.updateOne(
        { _id: user._id },
        { $inc: { balance: price + commission, commission: commission, commissionToday: commission } }
      );

      const taskUpdatePromise = Task.updateOne(
        { _id: task._id },
        { $set: { status: 'Completed', completedAt: now, 'product.commission': commission } }
      );

      await Promise.all([userUpdatePromise, taskUpdatePromise]);

      // Fire-and-forget referral distribution (async) so we don't block the response
      (async () => {
        try {
          const sourceRef = `task:${task._id}:completed`;
          await distributeReferralCommission({
            sourceUserId: user._id,
            originalAmount: commission,
            sourceReference: sourceRef,
            sourceType: 'task',
            note: `Referral from task ${task._id}`
          });
        } catch (err) {
          console.error('Referral distribution failed (single, async):', err);
        }
      })();

      // After marking this task completed, check whether the set is finished
      try {
        const taskSet = task.set || 1;
        const completedCount = await Task.countDocuments({ username: user.username, set: taskSet, status: { $regex: /^completed$/i } });
        if (completedCount >= (vipInfo.tasks || 40)) {
          const todayKey = getUKDateKey();
          // Atomically increment registeredWorkingDays[todayKey] and set resetRequested flag.
          // IMPORTANT: do NOT auto-increment currentSet anymore.
          const updates = {
            $inc: { [`registeredWorkingDays.${todayKey}`]: 1 },
            $set: { setStartingBalance: null, resetRequested: true }
          };
          await User.updateOne({ _id: user._id }, updates);
          // do not increment currentSet automatically here
        }
      } catch (err) {
        console.error('post-task-completion bookkeeping failed:', err);
      }

      // Build response locally to avoid extra DB read
      const responseTask = {
        ...task,
        status: 'Completed',
        completedAt: now,
        product: {
          ...task.product,
          commission
        }
      };

      return res.json({ success: true, task: responseTask });
    } catch (err) {
      console.error('submit-task error:', err);
      return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    }
});
// ----------------------- Admin Endpoint: Reset User Task Set -----------------------
router.post('/admin/reset-user-task-set', async (req, res) => {
    const { username, adminSecret } = req.body;
    const ADMIN_SECRET = 'yoursecretpassword';
    if (adminSecret !== ADMIN_SECRET) {
        return res.status(403).json({ success: false, message: 'Unauthorized' });
    }
    const user = await User.findOne({ username });
    if (!user) {
        return res.json({ success: false, message: 'User not found.' });
    }
    if (typeof user.currentSet !== "number") user.currentSet = 1;
    user.currentSet += 1;
    // Clear setStartingBalance so next set will record a fresh starting capital
    await User.updateOne({ _id: user._id }, { $set: { currentSet: user.currentSet, setStartingBalance: null } });
    res.json({ success: true, message: 'User task set has been reset. They can start a new set now.' });
});

// ----------------------- Admin Endpoint: Set Platform Status (NEW) -----------------------
router.post('/admin/set-platform-status', async (req, res) => {
    const { closed, autoOpenHourUK, allowList, autoOpenTime, adminSecret } = req.body;
    const ADMIN_SECRET = 'yoursecretpassword';
    if (adminSecret !== ADMIN_SECRET) {
        return res.status(403).json({ success: false, message: 'Unauthorized' });
    }

    let settings = await getOrCreateSettings();

    const updates = {};
    if (typeof closed === 'boolean') updates.platformClosed = closed;

    // Accept autoOpenHourUK as number OR autoOpenTime ("HH:MM")
    if (autoOpenHourUK !== undefined && !isNaN(Number(autoOpenHourUK))) {
      updates.autoOpenHourUK = Number(autoOpenHourUK);
    } else if (typeof autoOpenTime === 'string' && autoOpenTime.trim()) {
      const parts = autoOpenTime.split(':');
      const parsed = parseInt(parts[0], 10);
      if (!isNaN(parsed) && parsed >= 0 && parsed <= 23) updates.autoOpenHourUK = parsed;
    }

    // allowList can be array or comma-separated string; store into whoCanAccessDuringClose
    if (Array.isArray(allowList)) {
      updates.whoCanAccessDuringClose = allowList;
    } else if (typeof allowList === 'string' && allowList.trim()) {
      updates.whoCanAccessDuringClose = allowList.split(',').map(s => s.trim()).filter(Boolean);
    }

    if (Object.keys(updates).length) {
      await Setting.updateOne({ _id: settings._id }, { $set: updates });
      settings = await Setting.findById(settings._id);
    }

    res.json({ success: true, settings: {
      platformClosed: !!settings.platformClosed,
      autoOpenHourUK: typeof settings.autoOpenHourUK === 'number' ? settings.autoOpenHourUK : 10,
      whoCanAccessDuringClose: Array.isArray(settings.whoCanAccessDuringClose) ? settings.whoCanAccessDuringClose : []
    }});
});

// ----------------------- Admin Endpoint: Update User Credit Score (NEW) -----------------------
router.patch('/admin/users/:userId/credit_score', async (req, res) => {
  const { adminSecret, creditScore } = req.body;
  const ADMIN_SECRET = 'yoursecretpassword';
  if (adminSecret !== ADMIN_SECRET) {
    return res.status(403).json({ success: false, message: 'Unauthorized' });
  }

  const val = Number(creditScore);
  if (!Number.isFinite(val) || val < 0 || val > 100) {
    return res.status(400).json({ success: false, message: 'creditScore must be a number between 0 and 100.' });
  }

  const userIdArg = String(req.params.userId || '').trim();
  if (!userIdArg) return res.status(400).json({ success: false, message: 'Missing user identifier in URL.' });

  let user = null;
  // Try object id first
  try {
    if (mongoose.Types.ObjectId.isValid(userIdArg)) {
      user = await User.findById(userIdArg);
    }
  } catch (err) {
    // ignore
  }
  // Fallback to username lookup
  if (!user) {
    user = await User.findOne({ username: userIdArg });
  }
  if (!user) {
    return res.status(404).json({ success: false, message: 'User not found.' });
  }

  // Set both new and legacy fields if present
  user.creditScore = val;
  try {
    // keep legacy compatibility if code references credit_score
    user.credit_score = val;
  } catch (e) {
    // ignore if strict prevents it
  }

  await user.save();

  // Audit log entry (best-effort)
  try {
    await Log.create({
      type: 'admin_credit_update',
      admin: 'admin', // we don't store admin identity here because this endpoint uses adminSecret
      username: user.username,
      userId: String(user._id),
      newCreditScore: val,
      createdAt: new Date().toISOString()
    });
  } catch (e) {
    console.warn('Failed to create credit update log:', e && e.message ? e.message : e);
  }

  return res.json({ success: true, message: 'Credit score updated.', user: { username: user.username, id: user._id, creditScore: val } });
});

// ----------------------- Admin Endpoint: Invalidate All Tokens (NEW) -----------------------
// Admin can trigger this to permanently invalidate tokens issued before now for all users.
// Useful to quickly revoke access across all devices (e.g., after compromise).
router.post('/admin/invalidate-all-tokens', async (req, res) => {
  const { adminSecret } = req.body;
  const ADMIN_SECRET = 'yoursecretpassword';
  if (adminSecret !== ADMIN_SECRET) {
    return res.status(403).json({ success: false, message: 'Unauthorized' });
  }

  try {
    const settings = await getOrCreateSettings();
    const ts = Date.now();
    await Setting.updateOne({ _id: settings._id }, { $set: { globalTokenInvalidBefore: ts } });
    // Optionally clear tokens on users in background (best-effort)
    // await User.updateMany({}, { $set: { token: "", tokenIssuedAt: null } });
    return res.json({ success: true, message: 'All tokens invalidated.', globalTokenInvalidBefore: ts });
  } catch (err) {
    console.error('invalidate-all-tokens error:', err);
    return res.status(500).json({ success: false, message: 'Failed to invalidate tokens' });
  }
});

// ----------------------- Admin Endpoint: Invalidate Single User's Tokens (NEW) -----------------------
router.post('/admin/invalidate-user-token', async (req, res) => {
  const { adminSecret, username, userId } = req.body;
  const ADMIN_SECRET = 'yoursecretpassword';
  if (adminSecret !== ADMIN_SECRET) {
    return res.status(403).json({ success: false, message: 'Unauthorized' });
  }

  try {
    let user = null;
    if (userId && mongoose.Types.ObjectId.isValid(String(userId))) {
      user = await User.findById(userId);
    }
    if (!user && username) {
      user = await User.findOne({ username });
    }
    if (!user) return res.status(404).json({ success: false, message: 'User not found' });

    const ts = Date.now();
    user.tokenInvalidBefore = ts;
    // Optionally clear existing token field so immediate effect for those holding same token string
    user.token = "";
    user.tokenIssuedAt = null;
    await user.save();

    return res.json({ success: true, message: 'User tokens invalidated.', username: user.username, tokenInvalidBefore: ts });
  } catch (err) {
    console.error('invalidate-user-token error:', err);
    return res.status(500).json({ success: false, message: 'Failed to invalidate user tokens' });
  }
});

// Deposit
router.post('/deposit', verifyUserToken, async (req, res) => {
    const { amount } = req.body;
    const user = req.user;
    if (!amount || isNaN(amount) || Number(amount) <= 0) {
        return res.json({ success: false, message: "Invalid amount" });
    }
    user.balance = (user.balance || 0) + Number(amount);

    await Deposit.create({
        username: user.username,
        amount: Number(amount),
        createdAt: new Date().toISOString(),
        status: "Completed"
    });

    await user.save();

    // notify profileContext clients to refresh canonical profile across devices
    try { /* best-effort: dispatch event to same-page clients */ } catch (e) {}

    res.json({ success: true, user: { username: user.username, balance: user.balance, commissionToday: user.commissionToday } });
});

// Withdraw
// Middleware checkPlatformStatus applied here to block when platformClosed.
router.post('/withdraw', verifyUserToken, checkPlatformStatus, async (req, res) => {
    const { amount, withdrawPassword } = req.body;
    const user = req.user;

    if (!amount || isNaN(amount) || Number(amount) <= 0) {
        return res.json({ success: false, message: "Invalid amount" });
    }
    if (!withdrawPassword) {
        return res.json({ success: false, message: "Withdrawal password required" });
    }
    let actualWithdrawPwd = user.withdrawPassword || user.withdrawalPassword;
    if (!actualWithdrawPwd || actualWithdrawPwd !== withdrawPassword) {
        return res.json({ success: false, message: "Incorrect withdrawal password." });
    }

    // Enforce credit score requirement server-side (must be 100 to withdraw)
    const currentCredit = Number(user.creditScore ?? user.credit_score ?? 100);
    if (currentCredit < 100) {
      return res.json({ success: false, message: "Please update your credit score to proceed with the withdrawals." });
    }

    if (Number(amount) > (user.balance || 0)) {
        return res.json({ success: false, message: "Insufficient balance" });
    }
    user.balance -= Number(amount);

    await Withdrawal.create({
        id: crypto.randomBytes(12).toString('hex'),
        username: user.username,
        amount: Number(amount),
        createdAt: new Date().toISOString(),
        status: "Pending"
    });

    await user.save();

    // notify profileContext clients (best-effort)
    try { /* no-op */ } catch (e) {}

    res.json({ success: true, user: { username: user.username, balance: user.balance } });
});

// Transactions
router.get('/transactions', verifyUserToken, async (req, res) => {
    const user = req.user;
    const deposits = await Deposit.find({ username: user.username });

    let adminTransactions = [];
    try {
        const allTransactions = await Transaction.find({ $or: [{ user: user.username }, { username: user.username }] });
        adminTransactions = allTransactions.filter(
            tx =>
                (tx.type === "admin_add_balance" || tx.type === "admin_add_funds" || tx.type === "add_balance_admin")
        ).map(tx => ({
            username: tx.user || tx.username,
            amount: tx.amount,
            createdAt: tx.createdAt || tx.date || new Date().toISOString(),
            status: tx.status || "Completed",
            type: tx.type || "admin_add_balance",
            id: tx.id
        }));
    } catch (err) {
        adminTransactions = [];
    }

    const allDeposits = [
        ...deposits.map(d => ({ ...d.toObject(), type: "deposit" })),
        ...adminTransactions
    ].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    const withdrawals = await Withdrawal.find({ username: user.username });

    res.json({ success: true, deposits: allDeposits, withdrawals });
});

// Verify withdraw password
router.post('/verify-withdraw-password', verifyUserToken, async (req, res) => {
    const { password } = req.body;
    const user = req.user;

    let actualWithdrawPwd = user.withdrawPassword || user.withdrawalPassword;

    if (!actualWithdrawPwd) {
        return res.json({ success: false, message: "No withdrawal password is set." });
    }
    if (actualWithdrawPwd === password) {
        return res.json({ success: true });
    } else {
        return res.json({ success: false, message: "Incorrect withdrawal password." });
    }
});

// Change password
router.post('/change-password', verifyUserToken, async (req, res) => {
    const { oldPassword, newPassword } = req.body;
    // Re-fetch the latest user doc from DB
    const user = await User.findById(req.user._id);

    if (!user.loginPassword || user.loginPassword !== oldPassword) {
        return res.json({ success: false, message: "Old password is incorrect." });
    }

    user.loginPassword = newPassword;
    user.token = ""; // Invalidate token on password change (force re-login across devices)
    user.tokenIssuedAt = null;
    await user.save();

    res.json({ success: true, message: "Password updated successfully. Please log in again." });
});

// Change withdraw password
router.post('/change-withdraw-password', verifyUserToken, async (req, res) => {
    const { oldPassword, newPassword } = req.body;
    const user = req.user;

    let current = user.withdrawPassword || user.withdrawalPassword;
    if (!current || current !== oldPassword) {
        return res.json({ success: false, message: "Old withdrawal password is incorrect." });
    }
    user.withdrawPassword = newPassword;
    if (user.withdrawalPassword) user.withdrawalPassword = undefined;

    try {
        await user.save();
        res.json({ success: true, message: "Withdrawal password updated successfully." });
    } catch (err) {
        res.status(500).json({ success: false, message: "Failed to save new withdrawal password. Try again later." });
    }
});

// Notifications
router.get('/notifications', verifyUserToken, async (req, res) => {
    const notifications = await Notification.find({}).sort({ date: -1 });
    res.json({ success: true, notifications });
});

router.post('/admin/notification', async (req, res) => {
    const { title, message } = req.body;
    await Notification.create({
        id: Date.now(),
        title,
        message,
        date: new Date().toISOString()
    });
    res.json({ success: true });
});

// ----------------------- Translation endpoint (dynamic, cached) -----------------------
// This endpoint returns a JSON object for the requested namespace (ns) and language (lng).
// Behaviour:
// 1. If a translations document exists in the `translations` collection for {lng, ns} it is returned.
// 2. If not, it will attempt to locate an English source (lng='en') for the same ns and:
//    - if a translation provider API key is set (DEEPL_API_KEY or GOOGLE_API_KEY) it will auto-translate values,
//      cache the result in `translations` collection and return it.
//    - otherwise it will return the English source (fallback) or an empty object.
//
// Notes:
// - To enable automatic translation, set process.env.DEEPL_API_KEY or process.env.GOOGLE_API_KEY on your server.
router.get('/translate', async (req, res) => {
  const { lng, ns } = req.query;
  if (!lng || !ns) {
    return res.status(400).json({ success: false, message: "Missing 'lng' or 'ns' query parameter" });
  }

  try {
    const Translation = mongoose.models.Translation || mongoose.model('Translation',
      new mongoose.Schema({ lng: String, ns: String, data: Object }, { collection: 'translations', strict: false })
    );

    // 1) If we already have cached translations for this lang/namespace, return them
    const existing = await Translation.findOne({ lng, ns });
    if (existing && existing.data && Object.keys(existing.data).length) {
      return res.json(existing.data);
    }

    // 2) Otherwise attempt to use an English base (if available)
    const baseDoc = await Translation.findOne({ lng: 'en', ns });
    const baseData = baseDoc && baseDoc.data ? baseDoc.data : {};

    // If requested English or no base data, return base (maybe empty)
    if (lng === 'en') {
      return res.json(baseData);
    }
    if (!Object.keys(baseData).length) {
      // Nothing to translate - return empty object
      return res.json({});
    }

    // Prepare keys and texts for translation
    const keys = Object.keys(baseData);
    const texts = keys.map(k => String(baseData[k] || ''));

    let translatedData = {};

    // 3) Use DeepL if configured
    if (process.env.DEEPL_API_KEY) {
      try {
        const params = new URLSearchParams();
        params.append('auth_key', process.env.DEEPL_API_KEY);
        texts.forEach(t => params.append('text', t));
        // DeepL expects uppercase language codes like "IT"
        params.append('target_lang', String(lng).toUpperCase());

        const resp = await axios.post('https://api-free.deepl.com/v2/translate', params.toString(), {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
        });

        const translations = (resp.data && resp.data.translations) ? resp.data.translations.map(t => t.text) : [];
        keys.forEach((k, i) => {
          translatedData[k] = translations[i] || texts[i];
        });
      } catch (err) {
        console.error('DeepL translation error:', err && err.message ? err.message : err);
        // fallback to baseData
        translatedData = { ...baseData };
      }
    }
    // 4) Use Google Translate if configured
    else if (process.env.GOOGLE_API_KEY) {
      try {
        const resp = await axios.post(`https://translation.googleapis.com/language/translate/v2?key=${process.env.GOOGLE_API_KEY}`, {
          q: texts,
          target: lng
        });
        const translations = (resp.data && resp.data.data && resp.data.data.translations) ? resp.data.data.translations.map(t => t.translatedText) : [];
        keys.forEach((k, i) => {
          translatedData[k] = translations[i] || texts[i];
        });
      } catch (err) {
        console.error('Google Translate error:', err && err.message ? err.message : err);
        translatedData = { ...baseData };
      }
    } else {
      // No translation provider configured — return baseData as fallback
      translatedData = { ...baseData };
    }

    // Cache translatedData for future requests (best-effort)
    try {
      await Translation.updateOne({ lng, ns }, { $set: { data: translatedData } }, { upsert: true });
    } catch (e) {
      console.warn('Failed to cache translations:', e && e.message ? e.message : e);
    }

    return res.json(translatedData);
  } catch (err) {
    console.error('translate endpoint error:', err && err.message ? err.message : err);
    return res.status(500).json({ success: false, message: 'Translation processing failed', error: err.message });
  }
});

module.exports = router;
