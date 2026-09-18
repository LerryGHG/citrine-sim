const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { randomUUID } = require("crypto");

const app = express();
app.use(express.json());
app.use(express.static("public"));

const server = http.createServer(app);

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const state = {
  connected: false,
  centralSystemUrl: "",
  stationId: "",
  connectorStatus: "Unavailable", // Available, Preparing, Charging, SuspendedEV, Finishing
  cablePlugged: false,
  carReady: true,
  currentDrawAmps: 16,
  idTag: null,
  transactionId: null,
  meterStartWh: 0,
  meterWh: 0,
  meterIntervalSeconds: 10,
  sessionStartedAt: null,
};

let ws = null;
let pendingCalls = new Map(); // messageId -> {resolve, reject}
let heartbeatTimer = null;
let meterTimer = null;
let messageLog = []; // {ts, dir: 'out'|'in', action, payload}
let sseClients = [];

function log(dir, action, payload) {
  const entry = { ts: new Date().toISOString(), dir, action, payload };
  messageLog.push(entry);
  if (messageLog.length > 500) messageLog.shift();
  broadcast({ type: "log", entry });
}

function broadcast(msg) {
  const data = `data: ${JSON.stringify(msg)}\n\n`;
  for (const res of sseClients) res.write(data);
}

function pushState() {
  broadcast({ type: "state", state });
}

// ---------------------------------------------------------------------------
// OCPP-J plumbing
// ---------------------------------------------------------------------------

function sendCall(action, payload) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      reject(new Error("Not connected"));
      return;
    }
    const messageId = randomUUID();
    pendingCalls.set(messageId, { resolve, reject, action });
    const frame = [2, messageId, action, payload];
    log("out", action, payload);
    ws.send(JSON.stringify(frame));
  });
}

function respondCallResult(messageId, payload) {
  ws.send(JSON.stringify([3, messageId, payload]));
}

function respondCallError(messageId, code, description) {
  ws.send(JSON.stringify([4, messageId, code, description, {}]));
}

// How the simulator answers messages the CSMS initiates. Anything not listed
// here gets a generic "NotImplemented" CallError rather than silently hanging.
const csmsRequestHandlers = {
  RemoteStartTransaction: async (payload) => {
    if (payload.idTag) {
      setTimeout(() => plugAndPresentTag(payload.idTag), 200);
    }
    return { status: "Accepted" };
  },
  RemoteStopTransaction: async () => {
    setTimeout(() => stopSession("Remote"), 200);
    return { status: "Accepted" };
  },
  GetConfiguration: async () => ({ configurationKey: [], unknownKey: [] }),
  ChangeConfiguration: async () => ({ status: "Accepted" }),
  ChangeAvailability: async () => ({ status: "Accepted" }),
  UnlockConnector: async () => ({ status: "Unlocked" }),
  Reset: async () => ({ status: "Accepted" }),
  TriggerMessage: async () => ({ status: "Accepted" }),
  GetDiagnostics: async () => ({}),
  DataTransfer: async () => ({ status: "UnknownVendorId" }),
};

async function handleIncoming(raw) {
  let frame;
  try {
    frame = JSON.parse(raw);
  } catch {
    return;
  }
  const [type] = frame;

  if (type === 2) {
    const [, messageId, action, payload] = frame;
    log("in", action, payload);
    const handler = csmsRequestHandlers[action];
    if (handler) {
      try {
        const result = await handler(payload);
        respondCallResult(messageId, result);
        log("out", `${action}.conf`, result);
      } catch (err) {
        respondCallError(messageId, "InternalError", String(err));
      }
    } else {
      respondCallError(messageId, "NotImplemented", `No handler for ${action}`);
    }
  } else if (type === 3) {
    const [, messageId, payload] = frame;
    const pending = pendingCalls.get(messageId);
    if (pending) {
      pendingCalls.delete(messageId);
      log("in", `${pending.action}.conf`, payload);
      pending.resolve(payload);
    }
  } else if (type === 4) {
    const [, messageId, code, description] = frame;
    const pending = pendingCalls.get(messageId);
    if (pending) {
      pendingCalls.delete(messageId);
      log("in", `${pending.action}.error`, { code, description });
      pending.reject(new Error(`${code}: ${description}`));
    }
  }
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

async function connect(centralSystemUrl, stationId) {
  if (ws) throw new Error("Already connected — disconnect first");

  const url = `${centralSystemUrl.replace(/\/+$/, "")}/${encodeURIComponent(stationId)}`;
  ws = new WebSocket(url, ["ocpp1.6"]);

  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });

  ws.on("message", (data) => handleIncoming(data.toString()));
  ws.on("close", () => {
    state.connected = false;
    state.connectorStatus = "Unavailable";
    clearInterval(heartbeatTimer);
    clearInterval(meterTimer);
    ws = null;
    pushState();
  });
  ws.on("error", (err) => {
    log("in", "WS_ERROR", { message: err.message });
  });

  state.centralSystemUrl = centralSystemUrl;
  state.stationId = stationId;
  state.connected = true;

  const bootResp = await sendCall("BootNotification", {
    chargePointVendor: "CitrineSim",
    chargePointModel: "CitrineSim CP",
    chargePointSerialNumber: `SIM-${stationId}`,
    firmwareVersion: "1.0.0",
  });

  if (bootResp.interval && bootResp.interval > 0) {
    heartbeatTimer = setInterval(() => {
      sendCall("Heartbeat", {}).catch(() => {});
    }, bootResp.interval * 1000);
  }

  state.connectorStatus = "Available";
  await sendCall("StatusNotification", {
    connectorId: 1,
    errorCode: "NoError",
    status: "Available",
  });

  pushState();
  return bootResp;
}

function disconnect() {
  if (ws) {
    ws.close();
    ws = null;
  }
  clearInterval(heartbeatTimer);
  clearInterval(meterTimer);
  state.connected = false;
  state.connectorStatus = "Unavailable";
  pushState();
}

// ---------------------------------------------------------------------------
// Session state machine — mirrors what real firmware does: it never claims
// to be charging unless the CSMS actually accepted the transaction.
// ---------------------------------------------------------------------------

async function setConnectorStatus(status) {
  state.connectorStatus = status;
  await sendCall("StatusNotification", {
    connectorId: 1,
    errorCode: "NoError",
    status,
  });
  pushState();
}

async function plugCable() {
  if (state.transactionId) return; // already charging, no-op
  state.cablePlugged = true;
  await setConnectorStatus("Preparing");
  if (state.idTag) {
    await tryStartTransaction();
  }
}

async function unplugCable() {
  if (state.transactionId) {
    await stopSession("EVDisconnected");
    return;
  }
  state.cablePlugged = false;
  state.idTag = null;
  await setConnectorStatus("Available");
}

async function plugAndPresentTag(idTag) {
  if (!state.cablePlugged) await plugCable();
  await presentTag(idTag);
}

async function presentTag(idTag) {
  if (state.transactionId) {
    // Tag presented again mid-session == tap-out
    await stopSession("Local");
    return;
  }

  const authResp = await sendCall("Authorize", { idTag });
  const status = authResp.idTagInfo && authResp.idTagInfo.status;

  if (status !== "Accepted") {
    // This is the behavior real firmware — and a correct simulator — must
    // have: a non-Accepted Authorize response means the session goes no
    // further. No StartTransaction, no fake "Charging" status.
    state.idTag = null;
    pushState();
    return { authorized: false, status };
  }

  state.idTag = idTag;
  if (state.cablePlugged) {
    return await tryStartTransaction();
  }
  pushState();
  return { authorized: true, status };
}

async function tryStartTransaction() {
  state.meterStartWh = state.meterWh;
  const startResp = await sendCall("StartTransaction", {
    connectorId: 1,
    idTag: state.idTag,
    meterStart: Math.round(state.meterWh),
    timestamp: new Date().toISOString(),
  });

  const status = startResp.idTagInfo && startResp.idTagInfo.status;
  const txId = startResp.transactionId;

  // Treat anything other than a genuinely-accepted, nonzero transaction id
  // as a rejection. This is exactly the check open-ocpp-simu got wrong
  // (it treated ConcurrentTx as if StartTransaction had succeeded).
  if (status !== "Accepted" || !txId) {
    state.idTag = null;
    await setConnectorStatus(state.cablePlugged ? "Preparing" : "Available");
    return { started: false, status };
  }

  state.transactionId = txId;
  state.sessionStartedAt = Date.now();
  await setConnectorStatus(state.carReady ? "Charging" : "SuspendedEV");
  startMeterLoop();
  return { started: true, status, transactionId: txId };
}

async function stopSession(reason) {
  if (!state.transactionId) return;
  clearInterval(meterTimer);
  meterTimer = null;

  const txId = state.transactionId;
  state.transactionId = null;
  const idTag = state.idTag;
  state.idTag = null;

  await sendCall("StopTransaction", {
    transactionId: txId,
    idTag: idTag || undefined,
    meterStop: Math.round(state.meterWh),
    timestamp: new Date().toISOString(),
    reason,
  });

  await setConnectorStatus(state.cablePlugged ? "Finishing" : "Available");
  if (state.cablePlugged) {
    setTimeout(async () => {
      if (!state.transactionId) {
        state.cablePlugged = false;
        await setConnectorStatus("Available");
      }
    }, 3000);
  }
}

function startMeterLoop() {
  clearInterval(meterTimer);
  const wattsDrawn = state.currentDrawAmps * 230; // single-phase assumption
  meterTimer = setInterval(async () => {
    if (!state.transactionId) return;
    const whPerTick = (wattsDrawn * state.meterIntervalSeconds) / 3600;
    state.meterWh += whPerTick;
    await sendCall("MeterValues", {
      connectorId: 1,
      transactionId: state.transactionId,
      meterValue: [
        {
          timestamp: new Date().toISOString(),
          sampledValue: [
            {
              value: String(Math.round(state.meterWh)),
              measurand: "Energy.Active.Import.Register",
              unit: "Wh",
            },
            {
              value: String(state.currentDrawAmps),
              measurand: "Current.Import",
              unit: "A",
            },
          ],
        },
      ],
    }).catch(() => {});
    pushState();
  }, state.meterIntervalSeconds * 1000);
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------

app.post("/api/connect", async (req, res) => {
  try {
    const { centralSystemUrl, stationId } = req.body;
    const bootResp = await connect(centralSystemUrl, stationId);
    res.json({ ok: true, bootResp });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.post("/api/disconnect", (req, res) => {
  disconnect();
  res.json({ ok: true });
});

app.post("/api/cable", async (req, res) => {
  try {
    if (req.body.plugged) await plugCable();
    else await unplugCable();
    res.json({ ok: true, state });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.post("/api/rfid", async (req, res) => {
  try {
    const result = await presentTag(req.body.idTag);
    res.json({ ok: true, result, state });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.post("/api/stop", async (req, res) => {
  try {
    await stopSession("Local");
    res.json({ ok: true, state });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.post("/api/car-ready", async (req, res) => {
  state.carReady = !!req.body.ready;
  if (state.transactionId) {
    await setConnectorStatus(state.carReady ? "Charging" : "SuspendedEV");
  }
  res.json({ ok: true, state });
});

app.post("/api/current", (req, res) => {
  state.currentDrawAmps = Number(req.body.amps) || 0;
  res.json({ ok: true, state });
});

// Manual/raw message send — for edge-case testing without the state machine
// getting in the way (e.g. sending Authorize for a tag you never intend to
// actually start a transaction with).
app.post("/api/manual", async (req, res) => {
  try {
    const { action, payload } = req.body;
    const result = await sendCall(action, payload || {});
    res.json({ ok: true, result });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.get("/api/state", (req, res) => {
  res.json({ state, log: messageLog.slice(-100) });
});

app.get("/api/events", (req, res) => {
  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  res.flushHeaders();
  sseClients.push(res);
  res.write(`data: ${JSON.stringify({ type: "state", state })}\n\n`);
  req.on("close", () => {
    sseClients = sseClients.filter((c) => c !== res);
  });
});

const PORT = process.env.PORT || 8420;
server.listen(PORT, () => {
  console.log(`citrine-sim running at http://localhost:${PORT}`);
});
