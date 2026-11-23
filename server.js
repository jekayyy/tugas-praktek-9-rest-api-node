const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || 'rahasia-iot-123';

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const db = new sqlite3.Database('./telemetry.db', (err) => {
  if (err) console.error('Gagal koneksi SQLite:', err.message);
  else console.log('Terhubung ke database SQLite (telemetry.db)');
});

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS readings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id TEXT DEFAULT 'sensor-01',
      temperature REAL NOT NULL,
      humidity REAL,
      is_alert INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value REAL NOT NULL
    )
  `);

  db.run(`INSERT OR IGNORE INTO config (key, value) VALUES ('max_temp', 35.0)`);
});

function broadcast(payload) {
  const data = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

function requireApiKey(req, res, next) {
  const key = req.header('X-API-Key');
  if (!key || key !== API_KEY) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized: X-API-Key tidak valid atau tidak ditemukan'
    });
  }
  next();
}

function getThreshold() {
  return new Promise((resolve, reject) => {
    db.get(`SELECT value FROM config WHERE key = 'max_temp'`, (err, row) => {
      if (err) reject(err);
      else resolve(row ? row.value : 35.0);
    });
  });
}

app.get('/api/telemetry', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 20;
    const threshold = await getThreshold();
    db.all(
      `SELECT * FROM readings ORDER BY id DESC LIMIT ?`,
      [limit],
      (err, rows) => {
        if (err) return res.status(500).json({ success: false, message: err.message });
        res.json({
          success: true,
          threshold,
          data: rows.reverse()
        });
      }
    );
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.post('/api/telemetry', requireApiKey, async (req, res) => {
  const { device_id = 'sensor-01', temperature, humidity = null } = req.body;

  if (typeof temperature !== 'number' || Number.isNaN(temperature)) {
    return res.status(400).json({
      success: false,
      message: 'Field "temperature" wajib berupa angka'
    });
  }

  try {
    const threshold = await getThreshold();
    const isAlert = temperature > threshold ? 1 : 0;

    db.run(
      `INSERT INTO readings (device_id, temperature, humidity, is_alert) VALUES (?, ?, ?, ?)`,
      [device_id, temperature, humidity, isAlert],
      function (err) {
        if (err) return res.status(500).json({ success: false, message: err.message });

        const record = {
          id: this.lastID,
          device_id,
          temperature,
          humidity,
          is_alert: Boolean(isAlert),
          threshold,
          created_at: new Date().toISOString()
        };

        broadcast({ type: 'telemetry', payload: record });

        let notification = null;
        if (isAlert) {
          notification = {
            level: 'WARNING',
            message: `Peringatan! Suhu ${device_id} (${temperature}°C) melewati batas ${threshold}°C`,
            temperature,
            threshold,
            device_id,
            timestamp: record.created_at
          };
          console.warn('[NOTIFIKASI]', notification.message);
          broadcast({ type: 'alert', payload: notification });
        }

        res.status(201).json({
          success: true,
          data: record,
          notification
        });
      }
    );
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.put('/api/threshold', requireApiKey, (req, res) => {
  const { maxTemp, threshold } = req.body;
  const newLimit = typeof maxTemp === 'number' ? maxTemp : threshold;

  if (typeof newLimit !== 'number' || Number.isNaN(newLimit)) {
    return res.status(400).json({
      success: false,
      message: 'Kirimkan batas suhu dalam format angka (contoh: { "maxTemp": 32 })'
    });
  }

  db.run(
    `INSERT INTO config (key, value) VALUES ('max_temp', ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    [newLimit],
    (err) => {
      if (err) return res.status(500).json({ success: false, message: err.message });

      const updateInfo = { maxTemp: newLimit, updatedAt: new Date().toISOString() };
      broadcast({ type: 'threshold', payload: updateInfo });

      res.json({
        success: true,
        message: `Batas suhu berhasil diubah menjadi ${newLimit}°C`,
        data: updateInfo
      });
    }
  );
});

wss.on('connection', (ws) => {
  console.log('Klien WebSocket terhubung');
  getThreshold().then((t) => {
    ws.send(JSON.stringify({ type: 'threshold', payload: { maxTemp: t } }));
  });
});

server.listen(PORT, () => {
  console.log(`Server berjalan di http://localhost:${PORT}`);
  console.log(`WebSocket aktif di ws://localhost:${PORT}`);
  console.log(`X-API-Key aktif: ${API_KEY}`);
});