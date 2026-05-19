'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { v4: uuidv4 } = require('uuid');
const { Kafka } = require('kafkajs');
const { createRxDatabase } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');

//Database
const orderSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id: { type: 'string', maxLength: 100 },
    userId: { type: 'string' },
    productId: { type: 'string' },
    quantity: { type: 'number' },
    status: { type: 'string', default: 'pending' }
  },
  required: ['id', 'userId', 'productId', 'quantity', 'status']
};

let dbPromise = createRxDatabase({
  name: 'orderdb',
  storage: getRxStorageMemory()
}).then(async db => {
  await db.addCollections({
    orders: {
      schema: orderSchema
    }
  });
  return db;
});

// Proto
const PROTO_PATH  = path.join(__dirname, '..', 'proto', 'order.proto');
const pkgDef      = loader.loadSync(PROTO_PATH, { keepCase: true });
const orderProto  = grpc.loadPackageDefinition(pkgDef).order;

//kafka
const kafka    = new Kafka({ clientId: 'order-service', brokers: ['localhost:9092'] });
const producer = kafka.producer();

async function initKafka() {
  await producer.connect();
  console.log('Order Service Kafka producer connected');
}

async function publishOrderCreated(payload) {
  await producer.send({
    topic: 'order-created',
    messages: [{ value: JSON.stringify(payload) }],
  });
  console.log('[Kafka] Published order-created:', payload);
}

// grpc handlers
async function CreateOrder(call, callback) {
  const { userId, productId, quantity } = call.request;
  if (!userId || !productId || !quantity) {
    return callback(null, { error: 'userId, productId, and quantity are required' });
  }
  const id = uuidv4();
  try {
    const db = await dbPromise;
    await db.orders.insert({
      id,
      userId,
      productId,
      quantity,
      status: 'pending'
    });
    // Publish Kafka event
    await publishOrderCreated({ orderId: id, userId, productId, quantity }).catch(console.error);
    callback(null, { id, userId, productId, quantity, status: 'pending', error: '' });
  } catch (err) {
    callback(null, { error: err.message });
  }
}

async function GetOrders(call, callback) {
  try {
    const db = await dbPromise;
    const docs = await db.orders.find().exec();
    const rows = docs.map(doc => doc.toJSON());
    callback(null, { orders: rows.map(r => ({ ...r, error: '' })), error: '' });
  } catch (err) {
    callback(null, { orders: [], error: err.message });
  }
}

// start grpc server
const server = new grpc.Server();
server.addService(orderProto.OrderService.service, { CreateOrder, GetOrders });

const PORT = '0.0.0.0:50053';
server.bindAsync(PORT, grpc.ServerCredentials.createInsecure(), async (err, port) => {
  if (err) { console.error(err); process.exit(1); }
  console.log(`✅  Order Service gRPC running on port ${port}`);
  await initKafka().catch(console.error);
});
