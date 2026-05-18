'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const sqlite3 = require('sqlite3').verbose();
const { v4: uuidv4 } = require('uuid');
const { Kafka } = require('kafkajs');

//Database
const db = new sqlite3.Database(path.join(__dirname, 'products.db'));

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS products (
      id       TEXT PRIMARY KEY,
      name     TEXT NOT NULL,
      category TEXT NOT NULL,
      price    REAL NOT NULL,
      stock    INTEGER NOT NULL DEFAULT 0
    )
  `);
});

//Proto
const PROTO_PATH    = path.join(__dirname, '..', 'proto', 'product.proto');
const pkgDef        = loader.loadSync(PROTO_PATH, { keepCase: true });
const productProto  = grpc.loadPackageDefinition(pkgDef).product;

//gRPC Handlers
function AddProduct(call, callback) {
  const { name, category, price, stock } = call.request;
  if (!['phone', 'pc'].includes(category)) {
    return callback(null, { error: 'category must be "phone" or "pc"' });
  }
  const id = uuidv4();
  db.run(
    'INSERT INTO products (id, name, category, price, stock) VALUES (?, ?, ?, ?, ?)',
    [id, name, category, price, stock],
    (err) => {
      if (err) return callback(null, { error: err.message });
      callback(null, { id, name, category, price, stock, error: '' });
    }
  );
}

function GetProducts(call, callback) {
  db.all('SELECT * FROM products', [], (err, rows) => {
    if (err) return callback(null, { products: [], error: err.message });
    callback(null, { products: rows.map(r => ({ ...r, error: '' })), error: '' });
  });
}

function GetProduct(call, callback) {
  const { id } = call.request;
  db.get('SELECT * FROM products WHERE id = ?', [id], (err, row) => {
    if (err)  return callback(null, { error: err.message });
    if (!row) return callback(null, { error: 'Product not found' });
    callback(null, { ...row, error: '' });
  });
}

//Kafka Consumer
const kafka    = new Kafka({ clientId: 'product-service', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'product-service-group' });

async function startKafkaConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-created', fromBeginning: false });
  console.log('Product Service Kafka consumer connected');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const { orderId, productId, quantity } = event;
      console.log(`[Kafka] order-created → orderId=${orderId}, productId=${productId}, qty=${quantity}`);

      db.run(
        'UPDATE products SET stock = MAX(0, stock - ?) WHERE id = ?',
        [quantity, productId],
        (err) => {
          if (err) console.error('[DB] stock update error:', err.message);
          else     console.log(`[DB] Stock updated for product ${productId}`);
        }
      );
    },
  });
}

//Start gRPC Server
const server = new grpc.Server();
server.addService(productProto.ProductService.service, { AddProduct, GetProducts, GetProduct });

const PORT = '0.0.0.0:50052';
server.bindAsync(PORT, grpc.ServerCredentials.createInsecure(), async (err, port) => {
  if (err) { console.error(err); process.exit(1); }
  console.log(`✅  Product Service gRPC running on port ${port}`);
  await startKafkaConsumer().catch(console.error);
});
