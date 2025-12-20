import "dotenv/config";
import WebSocket from "ws";
import express from "express";

const STREAM_URL = "wss://stream.aisstream.io/v0/stream";
const mmsi = "308015000";
const CMS_HOST_URL = process.env.CMS_HOST_URL;
const CMS_EMAIL = process.env.CMS_EMAIL;
const CMS_PASSWORD = process.env.CMS_PASSWORD;
const cmsCollectionUrl = CMS_HOST_URL
  ? new URL("/api/collections/ais-logs", CMS_HOST_URL).toString()
  : null;
const cmsLoginUrl = CMS_HOST_URL
  ? new URL("/api/users/login", CMS_HOST_URL).toString()
  : null;

let socket;
let minutesWaited = 0;
let isSocketOpen = false;
let reconnectTimer = null;
let reconnectAttempts = 0;
let inactivityTimer = null;
let missingCmsHostWarned = false;
let missingCmsCredsWarned = false;
let cmsAuthToken = null;
let pendingCmsLogin = null;

const app = express();
const port = process.env.PORT || 3000;

app.get("/health", (_req, res) => {
  const status = isSocketOpen ? "connected" : "disconnected";
  res.status(isSocketOpen ? 200 : 503).json({ websocket: status });
});

app.listen(port, () => {
  console.log(`Health endpoint listening on port ${port}`);
});

const startInactivityTimer = () => {
  if (inactivityTimer) {
    clearInterval(inactivityTimer);
  }

  inactivityTimer = setInterval(() => {
    minutesWaited += 5;
    console.log(`${minutesWaited} minutes since last message...`);
  }, 300_000);
};

const clearInactivityTimer = () => {
  if (!inactivityTimer) {
    return;
  }

  clearInterval(inactivityTimer);
  inactivityTimer = null;
};

const sendSubscription = () => {
  if (!process.env.AIS_STREAM_KEY) {
    console.log("Missing aisstream.io API key");
    return;
  }

  if (!socket || socket.readyState !== WebSocket.OPEN) {
    console.log("WebSocket not ready; unable to subscribe");
    return;
  }

  const subscriptionMessage = {
    APIKey: process.env.AIS_STREAM_KEY,
    BoundingBoxes: [
      [
        [-90, -180],
        [90, 180],
      ],
    ],
    FiltersShipMMSI: [mmsi],
    FilterMessageTypes: ["PositionReport"],
  };

  socket.send(JSON.stringify(subscriptionMessage));
  startInactivityTimer();
};

const scheduleReconnect = () => {
  if (reconnectTimer) {
    return;
  }

  const delay = Math.min(30_000, 1_000 * 2 ** reconnectAttempts);
  console.warn(`AIS stream disconnected. Reconnecting in ${delay / 1000}s...`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    reconnectAttempts += 1;
    connect();
  }, delay);
};

const handleOpen = () => {
  console.log("AIS stream websocket connected");
  isSocketOpen = true;
  minutesWaited = 0;
  reconnectAttempts = 0;
  sendSubscription();
};

const handleClose = (event) => {
  isSocketOpen = false;
  clearInactivityTimer();
  console.warn(`AIS stream websocket closed (code ${event.code}).`);
  scheduleReconnect();
};

const handleError = (err) => {
  console.error("AIS stream websocket error", err);
  scheduleReconnect();
};

const handleMessage = async (event) => {
  try {
    let aisMessage = JSON.parse(event.data.toString());
    minutesWaited = 0;
    console.log("AIS Message");
    console.log(aisMessage);

    if (aisMessage?.Message?.PositionReport) {
      const pr = aisMessage.Message.PositionReport;
      await persistPositionReport(pr);
    } else {
      throw new Error("Unexpected AIS report shape");
    }
  } catch (err) {
    console.log(err);
  }
};

const performAuthorizedRequest = async (requestFactory) => {
  if (!cmsAuthToken) {
    await ensureCmsSession();
  }

  if (!cmsAuthToken) {
    throw new Error("Missing Payload auth token");
  }

  let response = await requestFactory();

  if (response.status === 401 || response.status === 403) {
    cmsAuthToken = null;
    await ensureCmsSession();

    if (!cmsAuthToken) {
      throw new Error(
        "Missing Payload auth token after re-authentication; cannot complete request"
      );
    }

    response = await requestFactory();
  }

  return response;
};

const shouldPersistPositionReport = async (pr) => {
  if (!cmsCollectionUrl) {
    warnMissingCmsHost();
    return false;
  }

  if (!cmsAuthToken) {
    console.warn("Skipping duplicate check; missing auth token");
    return true;
  }

  const now = Date.now();
  const twelveHoursAgoIso = new Date(now - 12 * 60 * 60 * 1000).toISOString();
  const sixHoursAgoThreshold = now - 6 * 60 * 60 * 1000;

  const queryUrl = new URL(cmsCollectionUrl);
  queryUrl.searchParams.set("limit", "50");
  queryUrl.searchParams.set("sort", "-createdAt");
  queryUrl.searchParams.set("where[mmsi][equals]", mmsi);
  queryUrl.searchParams.set(
    "where[createdAt][greater_than_or_equal]",
    twelveHoursAgoIso
  );

  const fetchRecentLogs = () =>
    fetch(queryUrl.toString(), {
      headers: {
        Authorization: `Bearer ${cmsAuthToken}`,
      },
    });

  let response;

  try {
    response = await performAuthorizedRequest(fetchRecentLogs);
  } catch (error) {
    console.error("Failed to query recent AIS logs", error);
    return true;
  }

  if (!response.ok) {
    const body = await response.text();
    console.error(
      `Payload REST query failed: ${response.status} ${response.statusText} - ${body}`
    );
    return true;
  }

  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    console.error("Failed to parse Payload REST query response", error);
    return true;
  }

  const docs = Array.isArray(payload?.docs) ? payload.docs : [];

  const hasRecentLog = docs.some((doc) => {
    if (!doc?.createdAt) {
      return false;
    }

    return new Date(doc.createdAt).getTime() >= sixHoursAgoThreshold;
  });

  const hasSameLocation = docs.some((doc) => {
    const docLocation = doc?.location;
    if (!Array.isArray(docLocation) || docLocation.length < 2) {
      return false;
    }

    return (
      Number(docLocation[0]) === Number(pr.Latitude) &&
      Number(docLocation[1]) === Number(pr.Longitude)
    );
  });

  if (hasRecentLog) {
    console.log("Skipping AIS log: entry exists within last 6 hours");
    return false;
  }

  if (hasSameLocation) {
    console.log(
      "Skipping AIS log: identical location already stored within last 12 hours"
    );
    return false;
  }

  return true;
};

const persistPositionReport = async (pr) => {
  if (!cmsCollectionUrl || !cmsLoginUrl) {
    warnMissingCmsHost();
    return;
  }

  if (!CMS_EMAIL || !CMS_PASSWORD) {
    warnMissingCmsCreds();
    return;
  }

  try {
    await ensureCmsSession();
  } catch (error) {
    console.error("Failed to authenticate with Payload REST API", error);
    return;
  }

  if (!cmsAuthToken) {
    console.warn("Skipping Payload persistence; missing auth token");
    return;
  }

  let shouldPersist;
  try {
    shouldPersist = await shouldPersistPositionReport(pr);
  } catch (error) {
    console.error("Failed to evaluate AIS log duplication rules", error);
    return;
  }

  if (!shouldPersist) {
    return;
  }

  const postAisLog = () =>
    fetch(cmsCollectionUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${cmsAuthToken}`,
      },
      body: JSON.stringify({
        mmsi,
        location: [pr.Latitude, pr.Longitude],
        sog: pr.Sog,
        navigationalStatus: pr.NavigationalStatus,
        rateOfTurn: pr.RateOfTurn,
        trueHeading: pr.TrueHeading,
      }),
    });

  try {
    const response = await performAuthorizedRequest(postAisLog);

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `Payload REST create failed: ${response.status} ${response.statusText} - ${body}`
      );
    }
  } catch (error) {
    console.error("Failed to persist AIS log via Payload REST API", error);
  }
};

const warnMissingCmsHost = () => {
  if (missingCmsHostWarned) {
    return;
  }

  console.warn("Missing CMS_HOST_URL; skipping Payload persistence");
  missingCmsHostWarned = true;
};

const warnMissingCmsCreds = () => {
  if (missingCmsCredsWarned) {
    return;
  }

  console.warn(
    "Missing CMS_EMAIL or CMS_PASSWORD; skipping Payload persistence"
  );
  missingCmsCredsWarned = true;
};

const loginToCms = async () => {
  if (!cmsLoginUrl) {
    warnMissingCmsHost();
    return null;
  }

  if (!CMS_EMAIL || !CMS_PASSWORD) {
    warnMissingCmsCreds();
    throw new Error("Cannot authenticate without CMS credentials");
  }

  const response = await fetch(cmsLoginUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email: CMS_EMAIL,
      password: CMS_PASSWORD,
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `Payload login failed: ${response.status} ${response.statusText} - ${body}`
    );
  }

  const payload = await response.json();
  const token = payload?.token || payload?.jwt;

  if (!token) {
    throw new Error("Payload login response missing token");
  }

  cmsAuthToken = token;
  console.log("Authenticated with Payload CMS REST API");
  return token;
};

const ensureCmsSession = async () => {
  if (cmsAuthToken) {
    return cmsAuthToken;
  }

  if (!cmsLoginUrl) {
    warnMissingCmsHost();
    return null;
  }

  if (!CMS_EMAIL || !CMS_PASSWORD) {
    warnMissingCmsCreds();
    return null;
  }

  if (!pendingCmsLogin) {
    pendingCmsLogin = (async () => {
      await loginToCms();
      return cmsAuthToken;
    })();
    pendingCmsLogin.finally(() => {
      pendingCmsLogin = null;
    });
  }

  return pendingCmsLogin;
};

const connect = () => {
  console.log("Connecting to AIS stream...");
  socket = new WebSocket(STREAM_URL);
  socket.onopen = handleOpen;
  socket.onmessage = handleMessage;
  socket.onclose = handleClose;
  socket.onerror = handleError;
};

connect();
