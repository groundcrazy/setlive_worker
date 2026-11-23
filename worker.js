require("dotenv").config();

const { createClient } = require("@supabase/supabase-js");

// ======================================
// ENVIRONMENT
// ======================================

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  SETLIVE_API_KEY,
} = process.env;

const API_URL =
  "https://setlive.myanmarnode.com/api/v1/twod-result-stream";

const TABLE_NAME = "twod_live_stream";
const ROW_ID = 1;

const TIMEZONE = "Asia/Yangon";

const RECONNECT_DELAY_MS = 5000;
const CLOSED_CHECK_DELAY_MS = 30000;
const SESSION_MONITOR_INTERVAL_MS = 5000;

// ======================================
// VALIDATE ENVIRONMENT
// ======================================

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !SETLIVE_API_KEY) {
  console.error(
    "Missing environment variables. Check SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY and SETLIVE_API_KEY in .env."
  );
  process.exit(1);
}

// ======================================
// SUPABASE CLIENT
// ======================================

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
  }
);

// ======================================
// MYANMAR TIME
// ======================================

function getMyanmarTime() {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    weekday: "short",
    hourCycle: "h23",
  }).formatToParts(new Date());

  const get = (type) =>
    parts.find((part) => part.type === type)?.value;

  return {
    date: `${get("year")}-${get("month")}-${get("day")}`,
    hour: Number(get("hour")),
    minute: Number(get("minute")),
    weekday: get("weekday"),
  };
}

// ======================================
// BUSINESS DAY CHECK
// Saturday and Sunday are closed.
// No SET holiday calendar is used.
// ======================================

function isBusinessDay(time) {
  return !["Sat", "Sun"].includes(time.weekday);
}

// ======================================
// CHECK TRADING SESSION
// Morning:   10:00 - 12:05
// Afternoon: 13:00 - 16:35
// Myanmar time
// ======================================

function isSessionOpen() {
  const time = getMyanmarTime();

  if (!isBusinessDay(time)) {
    return false;
  }

  const currentMinutes = time.hour * 60 + time.minute;

  const morning =
    currentMinutes >= 600 &&
    currentMinutes < 725;

  const afternoon =
    currentMinutes >= 780 &&
    currentMinutes < 995;

  return morning || afternoon;
}

// ======================================
// SESSION NAME
// ======================================

function getSessionName() {
  const time = getMyanmarTime();
  const minutes = time.hour * 60 + time.minute;

  if (minutes >= 600 && minutes < 725) {
    return "MORNING";
  }

  if (minutes >= 780 && minutes < 995) {
    return "AFTERNOON";
  }

  return "CLOSED";
}

// ======================================
// DATABASE STATUS
// ======================================

async function updateStatus(status) {
  try {
    const { error } = await supabase
      .from(TABLE_NAME)
      .update({
        connection_status: status,
        updated_at: new Date().toISOString(),
      })
      .eq("id", ROW_ID);

    if (error) {
      console.error("Status Error:", error.message);
    }
  } catch (error) {
    console.error("Status Error:", error.message);
  }
}

// ======================================
// SAVE LIVE DATA
// ======================================

async function saveLiveData(payload) {
  try {
    const liveValue = payload?.data?.live;

    if (liveValue === undefined || liveValue === null) {
      console.warn("SSE update received without data.live; skipped.");
      return;
    }

    const { error } = await supabase
      .from(TABLE_NAME)
      .upsert(
        {
          id: ROW_ID,
          live: String(liveValue),
          raw_json: payload,
          connection_status: "connected",
          updated_at: new Date().toISOString(),
        },
        {
          onConflict: "id",
        }
      );

    if (error) {
      throw error;
    }

    console.log("2D Live Updated:", liveValue);

  } catch (error) {
    console.error("Save Error:", error.message);
  }
}

// ======================================
// SSE EVENT PARSER
// ======================================

async function processSSEEvent(eventText) {
  let eventName = "message";
  const dataLines = [];

  for (const line of eventText.split("\n")) {
    if (line.startsWith(":")) {
      continue;
    }

    if (line.startsWith("event:")) {
      eventName = line.slice(6).trim();
      continue;
    }

    if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).trimStart());
    }
  }

  if (eventName === "ping") {
    console.log("SSE Event: ping");
    return;
  }

  if (dataLines.length === 0) {
    return;
  }

  if (eventName !== "update" && eventName !== "message") {
    return;
  }

  try {
    const payload = JSON.parse(dataLines.join("\n"));
    await saveLiveData(payload);

  } catch (error) {
    console.error("JSON Parse Error:", error.message);
  }
}

// ======================================
// READ SSE STREAM
// ======================================

async function readSSEStream(response, signal) {
  if (!response.body) {
    throw new Error("Empty SSE response");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();

  let buffer = "";

  try {
    while (!signal.aborted && isSessionOpen()) {
      const { done, value } = await reader.read();

      if (done) {
        break;
      }

      buffer += decoder.decode(value, {
        stream: true,
      });

      buffer = buffer
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n");

      let separatorIndex;

      while ((separatorIndex = buffer.indexOf("\n\n")) !== -1) {
        const eventText = buffer.slice(0, separatorIndex);

        buffer = buffer.slice(separatorIndex + 2);

        await processSSEEvent(eventText);
      }
    }

    buffer += decoder.decode();

    if (buffer.trim()) {
      await processSSEEvent(buffer);
    }

  } finally {
    try {
      await reader.cancel();
    } catch {
      // Reader may already be closed.
    }

    try {
      reader.releaseLock();
    } catch {
      // Lock may already be released.
    }
  }
}

// ======================================
// CONNECT SSE
// ======================================

let activeController = null;

async function connectSSE() {
  const controller = new AbortController();

  activeController = controller;

  console.log("Connecting to SetLive API...");

  const scheduleMonitor = setInterval(() => {
    if (!isSessionOpen()) {
      console.log("Trading session ended. Closing SSE connection...");
      controller.abort();
    }
  }, SESSION_MONITOR_INTERVAL_MS);

  try {
    const response = await fetch(API_URL, {
      method: "GET",

      headers: {
        "X-API-Key": SETLIVE_API_KEY,
        Accept: "text/event-stream",
        "Cache-Control": "no-cache",
      },

      signal: controller.signal,
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    if (!response.body) {
      throw new Error("Empty SSE response");
    }

    console.log("SetLive SSE Connected");

    await updateStatus("connected");

    await readSSEStream(response, controller.signal);

  } finally {
    clearInterval(scheduleMonitor);

    if (activeController === controller) {
      activeController = null;
    }
  }
}

// ======================================
// MAIN WORKER
// ======================================

let running = true;

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function startWorker() {
  console.log("================================");
  console.log("SetLive Scheduled Worker Started");
  console.log("Timezone: Myanmar (UTC+6:30)");
  console.log("Trading days: Monday-Friday");
  console.log("Closed days: Saturday-Sunday");
  console.log("Morning: 10:00-12:05");
  console.log("Afternoon: 13:00-16:35");
  console.log("================================");

  while (running) {
    const time = getMyanmarTime();

    if (isSessionOpen()) {
      console.log(
        `[${time.date}] ${getSessionName()} SESSION ACTIVE`
      );

      try {
        await connectSSE();

      } catch (error) {
        if (running && error.name !== "AbortError") {
          console.error("SSE Error:", error.message);
          await updateStatus("disconnected");
        }
      }

      if (isSessionOpen() && running) {
        console.log("Reconnecting in 5 seconds...");
        await sleep(RECONNECT_DELAY_MS);
      }

    } else {
      const statusMessage = !isBusinessDay(time)
        ? "WEEKEND"
        : "OUTSIDE SCHEDULE";

      console.log(
        `[${time.date}] ${statusMessage} - Waiting...`
      );

      await updateStatus("disconnected");

      await sleep(CLOSED_CHECK_DELAY_MS);
    }
  }

  console.log("Worker Stopped");
}

// ======================================
// GRACEFUL SHUTDOWN
// ======================================

function shutdown(signal) {
  if (!running) {
    return;
  }

  console.log(`Received ${signal}. Shutting down...`);

  running = false;

  if (activeController) {
    activeController.abort();
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ======================================
// START WORKER
// ======================================

startWorker().catch((error) => {
  console.error("Fatal Error:", error);
  process.exit(1);
});