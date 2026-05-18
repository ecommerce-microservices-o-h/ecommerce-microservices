'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

//Proto
const PROTO_PATH         = path.join(__dirname, '..', 'proto', 'notification.proto');
const pkgDef             = loader.loadSync(PROTO_PATH, { keepCase: true });
const notificationProto  = grpc.loadPackageDefinition(pkgDef).notification;

//gRPC Handler
function HealthCheck(call, callback) {
  const { service } = call.request;
  console.log(`[HealthCheck] called for service: "${service}"`);
  callback(null, {
    status:  'OK',
    message: `Notification Service is healthy. Checked: ${service || 'N/A'}`,
  });
}

//Kafka Consumer
const kafka    = new Kafka({ clientId: 'notification-service', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'notification-service-group' });

async function startKafkaConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-created', fromBeginning: false });
  console.log('Notification Service Kafka consumer connected');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const { orderId, userId, productId, quantity } = event;
    },
  });
}

//Start gRPC Server
const server = new grpc.Server();
server.addService(notificationProto.NotificationService.service, { HealthCheck });

const PORT = '0.0.0.0:50054';
server.bindAsync(PORT, grpc.ServerCredentials.createInsecure(), async (err, port) => {
  if (err) { console.error(err); process.exit(1); }
  console.log(`Notification Service gRPC running on port ${port}`);
  await startKafkaConsumer().catch(console.error);
});
