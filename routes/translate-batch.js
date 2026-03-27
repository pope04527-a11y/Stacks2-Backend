const express = require("express");
const router = express.Router();

/**
 * POST /api/translate-batch
 * Body: { source: "en", target: "fr", items: ["Home","Menu", ...] }
 * Response: { success: true, translations: { "Home": "Accueil", ... } }
 *
 * Minimal safe implementation with an in-memory cache and per-item LibreTranslate calls.
 * For production, replace in-memory cache with Redis/Mongo and add rate-limiting.
 */

const LIBRETRANSLATE_URL =
  "https://libretranslate-production-c3f3.up.railway.app/translate";

// Simple in-memory cache with TTL (per process)
const cache = new Map();
// TTL in milliseconds (e.g. 24 hours)
const CACHE_TTL = 24 * 60 * 60 * 1000;

function getCacheKey(source, target, text) {
  return `${source}::${target}::${text}`;
}

function getCached(source, target, text) {
  const key = getCacheKey(source, target, text);
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    cache.delete(key);
    return null;
  }
  return entry.value;
}

function setCached(source, target, text, value) {
  const key = getCacheKey(source, target, text);
  cache.set(key, { value, expiresAt: Date.now() + CACHE_TTL });
}

router.post("/", async (req, res) => {
  const { source = "en", target, items } = req.body || {};

  if (!target || !Array.isArray(items)) {
    return res.status(400).json({
      success: false,
      message: "Invalid request. Expect { source, target, items: [] }",
    });
  }

  // dedupe and filter falsy
  const unique = Array.from(
    new Set(items.filter((it) => typeof it === "string" && it.trim() !== ""))
  );

  // prepare result map, prefill with cached values when available
  const translations = {};
  const toTranslate = [];

  for (const orig of unique) {
    const cached = getCached(source, target, orig);
    if (cached) {
      translations[orig] = cached;
    } else {
      toTranslate.push(orig);
    }
  }

  // If nothing to translate, return early
  if (toTranslate.length === 0) {
    return res.json({ success: true, translations });
  }

  try {
    // Fire off parallel requests for the missing items.
    const promises = toTranslate.map(async (text) => {
      try {
        const response = await fetch(LIBRETRANSLATE_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            q: text,
            source,
            target,
            format: "text",
          }),
        });

        if (!response.ok) {
          const txt = await response.text().catch(() => "");
          console.warn("LibreTranslate returned non-ok:", response.status, txt);
          return { orig: text, translated: text };
        }

        const data = await response.json();
        const translated = data && (data.translatedText || data.translated || data.translation) ? (data.translatedText || data.translated || data.translation) : text;
        // cache and return
        setCached(source, target, text, translated);
        return { orig: text, translated };
      } catch (err) {
        console.warn("LibreTranslate call failed for text:", text, err && err.message);
        return { orig: text, translated: text };
      }
    });

    const results = await Promise.all(promises);

    for (const r of results) {
      translations[r.orig] = r.translated;
    }

    // Ensure every requested key is present
    unique.forEach((u) => {
      if (!translations[u]) translations[u] = u;
    });

    return res.json({ success: true, translations });
  } catch (err) {
    console.error("translate-batch error:", err && err.message ? err.message : err);
    return res.status(500).json({ success: false, message: "Batch translation failed" });
  }
});

module.exports = router;
