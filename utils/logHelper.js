const fs = require("fs");
const path = require("path");

const logFile = path.join(__dirname, "../data/activityLog.json");

function appendLog(username, action) {
  const log = {
    username,
    action,
    date: new Date().toISOString(),
  };

  let logs = [];

  if (fs.existsSync(logFile)) {
    logs = JSON.parse(fs.readFileSync(logFile));
  }

  logs.unshift(log); // Add newest first

  fs.writeFileSync(logFile, JSON.stringify(logs, null, 2));
}

module.exports = { appendLog };