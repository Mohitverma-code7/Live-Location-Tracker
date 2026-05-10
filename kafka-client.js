import { Kafka } from 'kafkajs';

export const kafkaClient = new Kafka({
  clientId: 'chaicode',
  brokers: ['localhost:9092'],
});
