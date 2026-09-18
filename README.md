# CitrineSim

A purpose-built OCPP 1.6 charge point simulator for testing [CitrineOS](https://citrineos.io/), built after
hitting real bugs in several existing simulators (dead dependencies, and one that reported "Charging" even
after the CSMS rejected the transaction).

## Why this exists

Unlike message-sender-style simulators, CitrineSim mirrors what real firmware does: it never reports
`Charging` unless the CSMS actually accepted the transaction. A rejected `Authorize` or `StartTransaction`
(e.g. `Blocked`, `ConcurrentTx`) stops the flow right there — no fake status.

## Running it

```bash
npm install
node server.js
```

Then open `http://localhost:8420`.

## Features

- Full session flow: plug cable → present RFID tag → `Authorize` → `StartTransaction` → periodic
  `MeterValues` while charging → tap-out or unplug → `StopTransaction`
- Live, color-coded OCPP message log (every `→`/`←` frame)
- Responds to CSMS-initiated commands (`RemoteStartTransaction`, `RemoteStopTransaction`,
  `ChangeConfiguration`, `Reset`, etc.)
- Manual command panel for firing arbitrary raw OCPP messages outside the normal flow
- Configurable current draw, car-ready state, and Central System URL / Station ID per session

## Architecture

Single Node.js process (`server.js`) holding the actual OCPP-J WebSocket connection, with a small
Express + Server-Sent-Events API. `public/index.html` is a vanilla-JS frontend — no build step.
