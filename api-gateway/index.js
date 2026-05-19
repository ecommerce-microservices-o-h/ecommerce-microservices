'use strict';

const path   = require('path');
const express = require('express');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { ApolloServer, gql } = require('apollo-server-express');

// ── Proto Paths ───────────────────────────────────────────────────────────────
const PROTO_DIR = path.join(__dirname, '..', 'proto');

function loadClient(protoFile, packageName, serviceName, address) {
  const def = loader.loadSync(path.join(PROTO_DIR, protoFile), { keepCase: true });
  const pkg = grpc.loadPackageDefinition(def)[packageName];
  return new pkg[serviceName](address, grpc.credentials.createInsecure());
}

// ── gRPC Clients ──────────────────────────────────────────────────────────────
const userClient         = loadClient('user.proto',         'user',         'UserService',         'localhost:50051');
const productClient      = loadClient('product.proto',      'product',      'ProductService',      'localhost:50052');
const orderClient        = loadClient('order.proto',        'order',        'OrderService',        'localhost:50053');
const notificationClient = loadClient('notification.proto', 'notification', 'NotificationService', 'localhost:50054');

// ── Helper: promisify gRPC call ───────────────────────────────────────────────
function grpcCall(client, method, payload = {}) {
  return new Promise((resolve, reject) => {
    client[method](payload, (err, response) => {
      if (err) return reject(err);
      resolve(response);
    });
  });
}

// ── Express App ───────────────────────────────────────────────────────────────
const app = express();

// CORS must be registered BEFORE express.json() so preflight OPTIONS works
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json());

// ── Serve Frontend UI ─────────────────────────────────────────────────────────
// Open http://localhost:3000 in your browser after starting the gateway
app.use(express.static(path.join(__dirname, '..', 'frontend')));

// ── REST: Users ───────────────────────────────────────────────────────────────
app.post('/users', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'CreateUser', req.body);
    if (result.error) return res.status(400).json({ error: result.error });
    res.status(201).json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/users/:id', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'GetUser', { id: req.params.id });
    if (result.error) return res.status(404).json({ error: result.error });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── REST: Products ────────────────────────────────────────────────────────────
app.post('/products', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'AddProduct', req.body);
    if (result.error) return res.status(400).json({ error: result.error });
    res.status(201).json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/products', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'GetProducts', {});
    res.json(result.products || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/products/:id', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'GetProduct', { id: req.params.id });
    if (result.error) return res.status(404).json({ error: result.error });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── REST: Orders ──────────────────────────────────────────────────────────────
app.post('/orders', async (req, res) => {
  try {
    const result = await grpcCall(orderClient, 'CreateOrder', req.body);
    if (result.error) return res.status(400).json({ error: result.error });
    res.status(201).json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/orders', async (req, res) => {
  try {
    const result = await grpcCall(orderClient, 'GetOrders', {});
    res.json(result.orders || []);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── REST: Notification HealthCheck ────────────────────────────────────────────
app.get('/health/notification', async (req, res) => {
  try {
    const result = await grpcCall(notificationClient, 'HealthCheck', { service: 'notification-service' });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GraphQL Schema ────────────────────────────────────────────────────────────
const typeDefs = gql`
  type User {
    id: String
    name: String
    email: String
  }

  type Product {
    id: String
    name: String
    category: String
    price: Float
    stock: Int
  }

  type Order {
    id: String
    userId: String
    productId: String
    quantity: Int
    status: String
  }

  type Query {
    user(id: String!): User
    products: [Product]
    product(id: String!): Product
    orders: [Order]
  }
`;

const resolvers = {
  Query: {
    user: async (_, { id }) => {
      const r = await grpcCall(userClient, 'GetUser', { id });
      return r.error ? null : r;
    },
    products: async () => {
      const r = await grpcCall(productClient, 'GetProducts', {});
      return r.products || [];
    },
    product: async (_, { id }) => {
      const r = await grpcCall(productClient, 'GetProduct', { id });
      return r.error ? null : r;
    },
    orders: async () => {
      const r = await grpcCall(orderClient, 'GetOrders', {});
      return r.orders || [];
    },
  },
};

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function start() {
  const apollo = new ApolloServer({
    typeDefs,
    resolvers,
    // Disable introspection warning in dev
    csrfPrevention: false,
  });
  await apollo.start();

  // bodyParserConfig:false → tell Apollo NOT to run its own bundled body-parser
  // because Express 5 already parsed the body via express.json() above.
  // Without this, Apollo's raw-body tries to read an already-consumed stream → 500.
  apollo.applyMiddleware({ app, path: '/graphql', bodyParserConfig: false });

  const PORT = 3000;
  app.listen(PORT, () => {
    console.log(`✅  API Gateway REST    → http://localhost:${PORT}`);
    console.log(`✅  API Gateway GraphQL → http://localhost:${PORT}/graphql`);
    console.log('');
    console.log('REST endpoints:');
    console.log('  POST /users          - Create user');
    console.log('  GET  /users/:id      - Get user');
    console.log('  POST /products       - Add product');
    console.log('  GET  /products       - List products');
    console.log('  GET  /products/:id   - Get product');
    console.log('  POST /orders         - Create order');
    console.log('  GET  /orders         - List orders');
    console.log('  GET  /health/notification - Notification health check');
  });
}

start().catch(console.error);
