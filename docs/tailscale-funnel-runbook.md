# Tailscale Funnel runbook (P1b gate)

This repository cannot perform the operator steps below; they are recorded here
so the deployment is reproducible. They implement the
[Tailscale Funnel decision](../plans/azure-functions-runtime-exit-refactoring-plan.md#decision-tailscale-funnel-2026-11-16)
in the Azure Functions Runtime Exit plan.

## P0 — tailnet preparation (before any cutover)

1. Enable **MagicDNS** for the tailnet.
2. Enable **HTTPS certificates** for the tailnet (so the node gets a
   `*.ts.net` certificate managed by Tailscale).
3. In the tailnet policy, add a `funnel` node attribute **scoped to the server
   node only** — never to `autogroup:member`. Example ACL grant:

   ```json
   {
     "grants": [
       {
         "src": ["autogroup:member"],
         "dst": ["tag:server"],
         "ip": ["*:*"]
       }
     ],
     "tagOwners": {
       "tag:server": ["autogroup:admin"]
     }
   }
   ```

   Then, in the Tailscale admin console, enable Funnel for the node carrying the
   `tag:server` attribute. Confirm with:

   ```bash
   tailscale funnel status
   ```

## P0 — baseline capture

Before enabling any worker role, capture the comparison baseline (per-queue
depth, Cosmos RU/s, endpoint latencies):

```bash
./deploy/scripts/capture-baseline.sh
```

Store the output outside the repository. Repeat after each phase cutover.

## P1b — expose the home API

On the server node (not in this repository). **Funnel is enabled per port, not
per path**: port 443 may be either tailnet-only Serve or public Funnel, never
both. Any existing Serve routes on 443 become public when Funnel is enabled, so
this sequence explicitly clears them and moves the private routes to 8443 first.

1. Back up the current Serve config:

   ```bash
   tailscale funnel status --json > ~/tailscale-serve-backup.json
   ```

2. Clear the existing listener table (this removes the old `/` and
   `/obsidiandb` routes from 443):

   ```bash
   sudo tailscale funnel reset
   ```

3. Enable Funnel on 443, forwarding to the API container:

   ```bash
   sudo tailscale funnel --bg --https=443 8080
   ```

4. Restore the previously private routes on a separate tailnet-only port:

   ```bash
   sudo tailscale serve --bg --https=8443 --set-path=/ http://100.79.136.83:3000
   sudo tailscale serve --bg --https=8443 --set-path=/obsidiandb http://127.0.0.1:5984
   ```

5. Verify — the 443 entry must read `(Funnel on)`, the 8443 entry
   `(tailnet only)`:

   ```bash
   tailscale funnel status
   ```

6. Update every consumer of the moved private routes. In particular, the
   Obsidian Self-hosted LiveSync plugin stores its CouchDB URI **per device**:
   change it from `https://<machine>.<tailnet>.ts.net/obsidiandb` to
   `https://<machine>.<tailnet>.ts.net:8443/obsidiandb` on desktop *and* mobile.
   Keep the username, password, database name and end-to-end passphrase
   unchanged, and choose "use existing database" if the wizard offers to
   re-initialise.
   The app previously mounted at `/` also moves to port 8443: update bookmarks,
   monitoring checks and any links to `https://<machine>.<tailnet>.ts.net:8443/`.
   The bare hostname on 443 now belongs to the API Funnel.
   If that app is **Homepage** (gethomepage, commonly on port 3000), it validates
   the full Host header and will answer `400 {"error":"Host validation failed"}`
   until its compose environment includes the port-qualified name:

   ```yaml
   environment:
     HOMEPAGE_ALLOWED_HOSTS: "<machine>.<tailnet>.ts.net:8443,<existing entries>"
   ```

   Recreate the container with `docker compose up -d` — a plain `restart` keeps
   the old environment and the 400 persists.

   **Funnel target caveat.** `tailscale funnel ... 8080` forwards to
   `127.0.0.1:8080` **on the host**. The compose `api` service publishes that
   loopback port (`ports: ["127.0.0.1:8080:80"]`); only 127.0.0.1 is bound, so
   no public port or LAN exposure is added. Verify on the host with
   `curl http://127.0.0.1:8080/health` before trusting the public URL. If the
   API is not up yet, the funnel will return 502 — that is expected until the
   new compose file is deployed.

Public URL becomes `https://<machine>.<tailnet>.ts.net`. Then, as part of the
P1b cutover, perform the consumer-side changes listed in the plan:

- Point Trailscope's API base URL and MCP clients at the funnel URL.
- Update the Strava webhook subscription callback URL (same route path).
- Expect one re-login per user (session cookie hostname changed).
- Measure tile-endpoint bandwidth through the funnel relays (MVT/PMTiles are
  the workload to watch; API JSON and webhooks are safe).
- Optionally require auth on requests carrying `Tailscale-Funnel-Request: ?1`
  (Tailscale strips spoofed copies of this header).

No DNS cutover: `peakshunters.erikmagnusson.com/api` stays on the Azure Function
App until P3.

## Rollback

```bash
sudo tailscale funnel --https=443 off
```

Then restore the private routes from the backup, or re-run step 4 above.
DNS/proxy then points back at the still-deployed Azure API Function App.
