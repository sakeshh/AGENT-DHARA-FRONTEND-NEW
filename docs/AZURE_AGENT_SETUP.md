# Step-by-step: Where to find Azure AI Foundry agent details

The frontend talks to your **agent** (with actions, blob access, etc.), not to a deployed model. The app uses the **Agent API**: threads, messages, and runs.

You need: **endpoint**, **API key**, and **agent ID**. Use this guide to find them and fill `.env.local`.

---

## 1. Open your project in Azure AI Foundry

1. Go to **https://ai.azure.com** (or your Azure AI Foundry / AI Studio URL).
2. Sign in with your Azure account.
3. In the left sidebar, click **Projects** (or **Build** > **Projects**).
4. Open the **project** where your agent lives (the one you use in the playground).

---

## 2. Find the project endpoint (base URL)

The endpoint is the base URL used for API calls. You can get it in several ways.

### Option A – From Project overview

1. In the project, go to **Overview** (or **Project overview**).
2. Look for a section like **Project details**, **Endpoint**, or **API**.
3. You should see a URL like:
   ```text
   https://<YourFoundryResource>.services.ai.azure.com/api/projects/<YourProjectName>
   ```
4. Copy this **full URL**; it is your **project/base endpoint**.

### Option B – From Project settings

1. In the project, click the **gear icon** or **Settings**.
2. Open **Settings** or **Project settings**.
3. Look for **Endpoint**, **API endpoint**, or **Project URL**.
4. Copy the URL shown there.

### Option C – From Agent playground “View code”

1. Open your **agent** (Agents in the left menu, then click your agent).
2. Open the **Playground** tab.
3. Click **View code** (or **</> View code** / **Code**).
4. In the sample code you’ll see a **connection string** or **endpoint** variable.
5. The endpoint in that code is the base URL you need (sometimes with a path like `/openai/v1` or similar—copy what the sample uses).

**What to note:**  
- Write down the **exact URL** (including `https://` and any path like `/openai/v1` or `/api/...`).  
- This will be your **AZURE_AGENT_ENDPOINT** (or the base you use to build the full invoke URL in the next step).

---

## 3. Find the API key

1. In the **same project**, go to **Settings** (gear icon or “Settings” in the left/side menu).
2. Look for one of:
   - **Keys and endpoints**
   - **Keys and endpoint**
   - **Manage keys**
   - **Connected resources** (then a link to keys)
3. Open that section.
4. You’ll see:
   - **Endpoint** (you may have this from step 2).
   - **Keys** (e.g. Key 1, Key 2) or **API key**.
5. Click **Show** or **Copy** next to a key and copy it.
6. Store it somewhere safe; you’ll use it as **AZURE_AGENT_API_KEY**.

**If you don’t see Keys:**

- Your role may not have permission. You typically need **Owner**, **Contributor**, or **Cognitive Services Contributor** on the project/resource.
- Keys might be in **Azure Key Vault** linked to the project; an admin may need to grant you access or share the key.

---

## 4. Find your agent ID

The app invokes your **agent** by ID (threads + runs). You need this.

1. Go to **Agents** → click your agent (the one with blob actions).
2. **Playground** → **View code**: look for `agent_id`, `agentId`, or `assistant_id` and copy it.
3. Or check the browser URL when the agent is open (e.g. `.../agents/<agent-id>/...`).
4. Or in the agent **Settings** / **Overview**, find **Agent ID** or **ID**.

Put that value in `.env.local` as **AZURE_AGENT_ID**.

---

## 5. (Optional) Find the exact “invoke” URL for your agent

The **invoke** URL is what the app will call when the user sends a message. It might be the same as the project endpoint or a longer path.

### Option A – From “View code” in the Playground (recommended)

1. Open your **agent** → **Playground** tab.
2. Click **View code** (or **</>**).
3. In the sample you might see:
   - A variable like `endpoint` or `url` used in a `fetch()` or HTTP call.
   - Or a full URL in a comment (e.g. “Invoke URL: …”).
4. Copy that **full URL** (the one used to send the user message).  
   That is your **AZURE_AGENT_ENDPOINT** (the exact invoke URL).

### Option B – From the REST API pattern (threads + runs)

If the sample uses the “thread + run” pattern:

1. Base is usually:  
   `https://<resource>.services.ai.azure.com/api/projects/<project>`
2. A typical “create run” (invoke) URL looks like:  
   `POST …/threads/{threadId}/runs?api-version=...`  
   So the “base” you put in `.env` might be up to and including the project path; the code then adds `/threads/.../runs`.

If you use this pattern, we’ll need to adapt `lib/azure-agent.ts` to create a thread, add a message, create a run, and poll until the run completes. For a first connection, **prefer the URL from “View code”** if it’s a single POST to one endpoint.

---

## 6. See the request and response shape (optional)

So we can match the code in `lib/azure-agent.ts` to your agent:

### Request (what your agent expects)

1. In the **Playground**, open **View code**.
2. Find the **request body** (e.g. inside `fetch(..., { body: JSON.stringify(...) })`).
3. Note:
   - Is it `{ messages: [ { role: "user", content: "..." } ] }`?
   - Or `{ input: "..." }`, `{ query: "..." }`, or something else?
   - Which **headers** are used? (e.g. `Authorization: Bearer KEY` or `api-key: KEY`).

### Response (where the reply text is)

1. In the same sample, see how the **response** is read (e.g. `response.json()` then `data.something`).
2. Note the **path to the assistant’s reply**, for example:
   - `data.choices[0].message.content`
   - `data.output.text`
   - `data.result`
   - `data.message.content`

**Quick way:** Send one message in the Playground, open **browser DevTools** (F12) → **Network** tab, find the request that sends your message, and inspect:
- **Request URL** → exact invoke URL.
- **Request payload** → request body shape.
- **Response payload** → path to the reply text.

---

## 7. Fill in `.env.local` in this project

1. In the project root, copy the example file:
   ```bash
   copy .env.local.example .env.local
   ```
   (On macOS/Linux: `cp .env.local.example .env.local`.)

2. Open **.env.local** and set:

   ```env
   AZURE_AGENT_ENDPOINT=<project base URL from step 2>
   AZURE_AGENT_API_KEY=<API key from step 3>
   AZURE_AGENT_ID=<agent ID from step 4>
   ```

3. Save the file.  
   Do **not** commit `.env.local` (it’s in `.gitignore`).

---

## 8. If something doesn’t work

Share with your developer (or adjust in the code):

1. **Exact invoke URL** you put in `AZURE_AGENT_ENDPOINT`.
2. **Auth header**: e.g. `Authorization: Bearer <key>` or `api-key: <key>`.
3. **Request body**: e.g. “It expects `{ messages: [...] }`” or “It expects `{ input: "..." }`”.
4. **Response**: e.g. “The reply text is in `data.choices[0].message.content`”.

Then we can update `lib/azure-agent.ts` (and optionally `app/api/chat/route.ts`) to match your agent’s API.

---

## Quick checklist

| What to find        | Where to look                                      |
|---------------------|----------------------------------------------------|
| Project endpoint    | Project **Overview** or **Settings**                |
| API key             | Project **Settings** → **Keys and endpoints**      |
| **Agent ID**        | Agent page URL, **Playground** → **View code**, or agent **Settings** |

After you have the endpoint and key in `.env.local`, restart the Next.js dev server so it picks up the new env vars.
