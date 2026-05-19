'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const sqlite3 = require('sqlite3').verbose();
const { v4: uuidv4 } = require('uuid');

// ── Database ──────────────────────────────────────────────────────────────────
const db = new sqlite3.Database(path.join(__dirname, 'users.db'));

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id    TEXT PRIMARY KEY,
      name  TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE
    )
  `);
});

// ── Proto ─────────────────────────────────────────────────────────────────────
const PROTO_PATH = path.join(__dirname, '..', 'proto', 'user.proto');
const pkgDef     = loader.loadSync(PROTO_PATH, { keepCase: true });
const userProto  = grpc.loadPackageDefinition(pkgDef).user;

// ── gRPC Handlers ─────────────────────────────────────────────────────────────
function CreateUser(call, callback) {
  const { name, email } = call.request;
  if (!name || !email) {
    return callback(null, { error: 'name and email are required' });
  }
  const id = uuidv4();
  db.run('INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [id, name, email], (err) => {
    if (err) return callback(null, { error: err.message });
    callback(null, { id, name, email, error: '' });
  });
}

function GetUser(call, callback) {
  const { id } = call.request;
  db.get('SELECT * FROM users WHERE id = ?', [id], (err, row) => {
    if (err)  return callback(null, { error: err.message });
    if (!row) return callback(null, { error: 'User not found' });
    callback(null, { id: row.id, name: row.name, email: row.email, error: '' });
  });
}

// ── Start gRPC Server ─────────────────────────────────────────────────────────
const server = new grpc.Server();
server.addService(userProto.UserService.service, { CreateUser, GetUser });

const PORT = '0.0.0.0:50051';
server.bindAsync(PORT, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) { console.error(err); process.exit(1); }
  console.log(`✅  User Service gRPC running on port ${port}`);
});
