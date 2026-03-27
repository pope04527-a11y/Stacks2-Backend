const express = require("express");
const router = express.Router();

router.post("/", async (req, res) => {
  const { q, source, target } = req.body;

  try {
    const response = await fetch(
      "https://libretranslate-production-c3f3.up.railway.app/translate",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          q,
          source,
          target,
          format: "text"
        })
      }
    );

    const data = await response.json();

    res.json({
      success: true,
      translatedText: data.translatedText
    });
  } catch (err) {
    console.error("Translate error:", err.message);
    res.status(500).json({
      success: false,
      message: "Translation failed"
    });
  }
});

module.exports = router;
