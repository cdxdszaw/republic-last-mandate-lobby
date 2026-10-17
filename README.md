# Republic: Last Mandate — presence and chat

Minimal public deployment of the MIT-licensed presence server. No campaign data, game assets, private keys, or user saves are included.

Node.js 22+, npm ci --omit=dev, node server.mjs. Bind HOST=0.0.0.0 on managed hosting; PORT is supplied by the host. /health is the health endpoint and /lobby is the WebSocket endpoint. TLS must be provided by the hosting proxy. Free Render instances can sleep when idle and have monthly quotas. No continuous uptime guarantee.

The server stores the last 50 chat messages in RAM only. It sees connection IP addresses, pseudonyms, and public messages. No accounts or end-to-end encryption. Never put secrets or personal data in chat. The default application does not persist chat or visitor IPs to disk. Hosting logs and platform processing are separate.

MAX_CLIENTS is limited to 30 for this hobby deployment, not a load-test claim. TRUST_PROXY stays disabled until the hosting proxy trust boundary is verified. Clients behind a shared proxy may hit the per-IP connection limit; this is sufficient for the intended small group. No ADMIN_TOKEN is included in this public repository.
