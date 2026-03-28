// ==========================
// MONGODB CONNECTION SETUP
// ==========================
require("dotenv").config(); // load env early so process.env is available before connecting

const mongoose = require("mongoose");

// Prefer environment variable for URI; fall back to the existing literal (kept for compatibility)
const mongoURI =
  process.env.MONGODB_URI ||
  "mongodb+srv://Stacks2:Mark075555@stacks.surpuwe.mongodb.net/Stacks?retryWrites=true&w=majority&appName=Stacks";

// Optional explicit DB name via env (if you prefer not to include DB in URI)
const dbName = process.env.MONGODB_DB || undefined;

// Build connection options only when needed
const connectOpts = {};
if (dbName) connectOpts.dbName = dbName;

mongoose
  .connect(mongoURI, connectOpts)
  .then(() => {
    console.log("✅ Connected to MongoDB Atlas!");
    try {
      console.log(
        "MongoDB DB Name:",
        mongoose.connection && mongoose.connection.name
          ? mongoose.connection.name
          : "(unknown)"
      );
    } catch (e) {
      // ignore
    }
  })
  .catch((err) => console.error("❌ MongoDB connection error:", err));

const express = require("express");
const cors = require("cors");
const path = require("path");

// ===============================
// 🌩️ Load ENV Variables
// ===============================
require("dotenv").config();

const app = express();

// ===============================
// 🩺 Health Check
// ===============================
app.get("/health", require("./src/health"));

// =======================================
// ✅ CORS and JSON Middleware (MUST COME FIRST)
// =======================================
app.use(
  cors({
    origin: [
      "https://stackswork.netlify.app",
      "https://stacksl.com",
      "https://www.stacksl.com",
      "https://stacksapp.pages.dev",
      "http://localhost:5173",
      "https://stacks-48in.onrender.com",
      "https://stacksl.netlify.app",
    ],
    credentials: true,
    allowedHeaders: [
      "Content-Type",
      "Authorization",
      "X-Admin-Token",
      "X-Auth-Token",
      "x-admin-secret",
    ],
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    preflightContinue: false,
    optionsSuccessStatus: 204,
  })
);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// =======================================
// 📁 Serve Static Product Images
// =======================================
app.use(
  "/assets",
  express.static(path.join(__dirname, "public", "assets"), {
    maxAge: "30d",
    setHeaders: (res, filePath) => {
      if (
        filePath.endsWith(".png") ||
        filePath.endsWith(".jpg") ||
        filePath.endsWith(".jpeg") ||
        filePath.endsWith(".gif")
      ) {
        res.setHeader("Access-Control-Allow-Origin", "*");
      }
    },
  })
);

// =======================================
// 📁 Serve Favicon
// =======================================
app.use(
  "/favicon.ico",
  express.static(path.join(__dirname, "public", "favicon.ico"))
);

// =======================================
// 📁 Serve Static Admin Panel
// =======================================
app.use(
  "/admin-panel",
  express.static(path.join(__dirname, "public", "admin-panel"))
);

// =======================================
// 📁 Serve Static Frontend
// =======================================
app.use("/", express.static(path.join(__dirname, "public")));

// =======================================
// 🔗 API Routes
// =======================================
const apiRouter = require("./routes/api");
app.use("/api", apiRouter);

const adminRouter = require("./routes/admin");
app.use("/admin", adminRouter);

const processCommissionsRouter = require("./routes/process-commission");
app.use(processCommissionsRouter);

// =======================================
// ☁️ Cloudinary Upload
// =======================================
const uploadRouter = require("./routes/upload");
app.use("/api", uploadRouter);

// =======================================
// 🌍 LibreTranslate Proxy Route (ACTIVE)
// =======================================
const translateRouter = require("./routes/translate");
app.use("/api/translate", translateRouter);

const translateBatchRouter = require("./routes/translate-batch");
app.use("/api/translate-batch", translateBatchRouter);

// =======================================
// ❌ 404 Handler
// =======================================
app.use((req, res) => {
  res.status(404).json({ success: false, message: "Resource not found" });
});

// =======================================
// 🚨 Global Error Handler
// =======================================
app.use((err, req, res, next) => {
  console.error("❌ Unhandled error:", err);
  res.status(500).json({
    success: false,
    message: "Internal server error",
    error: err.message,
  });
});

// =======================================
// 🕛 Midnight Commission Reset
// =======================================
const cron = require("node-cron");

function getTodayDateString() {
  return new Date().toISOString().slice(0, 10);
}

const UserForCron = mongoose.models.User || null;

cron.schedule("0 0 * * *", async () => {
  const today = getTodayDateString();
  try {
    if (UserForCron) {
      await UserForCron.updateMany(
        {},
        { $set: { commissionToday: 0, lastCommissionReset: today } }
      );
      console.log("✅ Reset commissionToday for all users at midnight", today);
    } else {
      console.error("❌ User model not found for cron job.");
    }
  } catch (err) {
    console.error("❌ Cron error:", err);
  }
});

// =======================================
// 🚀 Start Server
// =======================================
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`✅ Backend running on port ${PORT}`);
});
