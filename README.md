## AgentDhara (Launch-ready)

This repo contains:
- **Frontend**: Next.js app (chat UI)
- **Backend**: FastAPI + LangGraph + MCP adapters (`Agent Dhara Backend`)

### Production requirements (no compromises)

#### Secrets & auth (required)
- Rotate any leaked keys and update environment variables.
- Set a strong shared secret for backend auth:
  - Backend: `BACKEND_AUTH_TOKEN`
  - Frontend: `BACKEND_AUTH_TOKEN` (same value)

#### Start backend (FastAPI)
From `Agent Dhara Backend/`:

```bash
python -m uvicorn agent.mcp_server:app --host 127.0.0.1 --port 8000
```

Health:
- `GET /healthz`
- `GET /readyz`

#### Start frontend (Next.js)
From repo root:

```bash
npm install
npm run dev
```

Frontend proxies to backend when `BACKEND_BASE_URL` is set.

#### CORS
Set backend `CORS_ALLOW_ORIGINS` (comma-separated) to your deployed frontend origins, for example:

```env
CORS_ALLOW_ORIGINS=https://yourdomain.com
```

#### Rate limiting
Set backend `RATE_LIMIT_PER_MINUTE` based on traffic.

### Chat commands
In `/chat`:
- `show sources`
- `list tables`
- `select table dbo.YourTable`
- `show schema`
- `preview`
- `what are the data quality issues?`

Notes:
- SQL execution is **SELECT-only**.
- Preview/query rows are **PII-masked** by default.
- Conversation memory is persisted in `Agent Dhara Backend/output/chat_sessions.sqlite3`.

---

## Next.js (reference)
This frontend was originally bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
