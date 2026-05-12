const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { createClient } = require('redis');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const cors = require('cors');

// Environment variables, mostly pulled from docker-compose.yml
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const LIVEKIT_API_KEY = process.env.LIVEKIT_API_KEY || 'devkey';
const LIVEKIT_API_SECRET = process.env.LIVEKIT_API_SECRET || 'secret';
const LIVEKIT_URL = process.env.LIVEKIT_URL || 'ws://localhost:7880';

// App Setup
const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

// Redis Setup
const redisClient = createClient({ url: REDIS_URL });
redisClient.on('error', (err) => console.log('Redis Client Error', err));

// LiveKit Setup
const roomService = new RoomServiceClient(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET);

async function start() {
  await redisClient.connect();
  console.log('Connected to Redis');

  io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Join the waiting pool for a specific slot
    socket.on('join_slot', async (data) => {
      const { slot } = data; // e.g. "4pm"
      const poolKey = `pool:slot:${slot}`;

      // Add user to the slot pool
      await redisClient.sAdd(poolKey, socket.id);
      console.log(`${socket.id} joined slot ${slot}`);

      // Basic Matching Logic for testing: if 2 people are in the pool, match them immediately.
      // In reality, this would be a T-0 batch job triggered by a cron/timer.
      const members = await redisClient.sMembers(poolKey);
      if (members.length >= 2) {
        // Pop two users
        const userA = members[0];
        const userB = members[1];

        // Remove from pool
        await redisClient.sRem(poolKey, userA);
        await redisClient.sRem(poolKey, userB);

        // Generate Room ID
        const roomName = `room-${Date.now()}`;

        console.log(`Matched! Creating LiveKit room: ${roomName}`);

        try {
          // Create LiveKit Room
          await roomService.createRoom({
            name: roomName,
            emptyTimeout: 10 * 60, // 10 minutes
            maxParticipants: 2,
          });

          // Generate Tokens for A and B
          const atA = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
            identity: userA,
            name: 'User A',
          });
          atA.addGrant({ roomJoin: true, room: roomName });
          const tokenA = await atA.toJwt();

          const atB = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
            identity: userB,
            name: 'User B',
          });
          atB.addGrant({ roomJoin: true, room: roomName });
          const tokenB = await atB.toJwt();

          // Notify Users
          io.to(userA).emit('matched', { roomName, token: tokenA, role: 'interviewer' });
          io.to(userB).emit('matched', { roomName, token: tokenB, role: 'interviewee' });

        } catch (error) {
          console.error("Error creating room or tokens:", error);
        }
      }
    });

    socket.on('disconnect', async () => {
      console.log(`User disconnected: ${socket.id}`);
      // In production, we'd need to iterate through pools and remove them, or use Redis Expiry/Sets properly.
      // For this simplified MVP, we will assume cleanup or use a more complex data structure later.
    });
  });

  server.listen(PORT, () => {
    console.log(`Matching Server running on port ${PORT}`);
  });
}

start().catch(console.error);