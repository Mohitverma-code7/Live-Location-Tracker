import { kafkaClient } from './kafka-client.js';

const locationHistoryByUser = new Map();
const processedEventIds = new Set();

async function init() {
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `database-processor`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      if (!data?.eventId || processedEventIds.has(data.eventId)) {
        await heartbeat();
        return;
      }

      processedEventIds.add(data.eventId);
      if (processedEventIds.size > 10000) {
        processedEventIds.clear();
      }

      const userId = String(data.id || 'unknown');
      const currentHistory = locationHistoryByUser.get(userId) || [];
      const nextHistory = [
        ...currentHistory,
        {
          latitude: data.latitude,
          longitude: data.longitude,
          updatedAt: data.updatedAt,
        },
      ].slice(-100);
      locationHistoryByUser.set(userId, nextHistory);

      console.log(`INSERT INTO DB LOCATION`, {
        userId,
        latest: nextHistory[nextHistory.length - 1],
        pointsStored: nextHistory.length,
      });
      await heartbeat();
    },
  });
}

init();
