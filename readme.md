# Live Location Tracker

Real-time location sharing app with Express, Socket.IO, Kafka, and OIDC authentication.

## OIDC Service Used

This project uses **my own OIDC service**:
- `https://token-shinobi.onrender.com/`

The backend reads OIDC discovery from:
- `https://token-shinobi.onrender.com/.well-known/openid-configuration`

## What This Project Does

- Authenticates users using OIDC authorization-code flow
- Validates access tokens with JWKS (`RS256`)
- Connects authenticated users to Socket.IO
- Streams live location updates through Kafka topic `location-updates`
- Broadcasts current users and live movement on Leaflet map

## Tech Stack

- Node.js (ESM)
- Express 5
- Socket.IO 4
- KafkaJS
- Apache Kafka (Docker for local)
- Leaflet + OpenStreetMap
- Browser Geolocation API

## Project Structure

```text
.
|- index.js
|- kafka-client.js
|- kafka-admin.js
|- database-processor.js
|- docker-compose.yml
|- env.sample
`- public/
   |- login.html
   |- auth.html
   `- index.html
```

## Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `PORT` | No | App port. Default: `5000` |
| `AUTH_ORIGIN` | Yes | OIDC base URL |
| `AUTHORIZATION_ENDPOINT` | No | Login endpoint. Default: `/user/login` (resolved with `AUTH_ORIGIN`) |
| `AUTH_REDIRECT_URI` | No | Callback URL. Default: `http://<host>/auth` |
| `AUTH_CLIENT_ID` | Yes | OIDC client ID |
| `AUTH_CLIENT_SECRET` | Yes | OIDC client secret |

Example:

```env
PORT=5000
AUTH_ORIGIN=https://token-shinobi.onrender.com
AUTHORIZATION_ENDPOINT=https://token-shinobi.onrender.com/user/login
AUTH_REDIRECT_URI=http://localhost:5000/auth
AUTH_CLIENT_ID=your-client-id
AUTH_CLIENT_SECRET=your-client-secret
```

## Run Locally

1. Install dependencies

```bash
npm install
```

2. Create env file

```powershell
Copy-Item env.sample .env
```

3. Start Kafka

```bash
docker compose up -d
```

4. Create topic

```bash
node kafka-admin.js
```

5. Start server

```bash
node --env-file=.env index.js
```

6. Open app

- `http://localhost:5000`

Optional consumer (DB simulation):

```bash
node database-processor.js
```

## Auth Flow (Code-Based)

1. User opens `/`
2. `public/login.html` checks saved token
3. If no valid token, app redirects to `/login`
4. Backend redirects to OIDC login endpoint
5. Provider returns to `/auth?code=...`
6. `public/auth.html` posts code to `/auth/exchange`
7. Backend exchanges code for token, validates JWT, returns `{ accessToken, user }`
8. Frontend stores session and opens `/home`

## Socket Events

Client -> Server:
- `client:location:update` `{ latitude, longitude }`

Server -> Client:
- `server:session` `{ socketId, user }`
- `server:users:snapshot` `{ users: [...] }`
- `server:location:update` `{ id, userName, latitude, longitude, updatedAt }`
- `server:error` `{ message }`

## Kafka Flow

- Topic: `location-updates`
- Producer: `index.js` (on `client:location:update`)
- Consumers:
  - `index.js` (rebroadcasts live updates)
  - `database-processor.js` (simulated persistence)

## HTTP Routes

- `GET /` -> login redirect page
- `GET /login` -> redirect to OIDC login
- `GET /auth` -> auth callback page
- `POST /auth/exchange` -> code exchange endpoint
- `GET /home` -> live map page
- `GET /health` -> health check

## Notes

- `AUTH_ORIGIN` should not have a trailing slash.
- Access token is stored in `localStorage` (demo approach).
- `database-processor.js` stores data in memory (not real DB).