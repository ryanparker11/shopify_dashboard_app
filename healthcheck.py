# /app/healthcheck.py
import os, socket, sys, http.client, time

port = int(os.environ.get("PORT", "8080"))

# try a few times during early boot
for _ in range(3):
    try:
        # quick TCP check (cheapest liveness)
        s = socket.create_connection(("127.0.0.1", port), 1.5)
        s.close()
        # optional HTTP check to /healthz; tolerate 2xx/3xx
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        conn.request("GET", "/healthz")
        r = conn.getresponse()
        sys.exit(0 if 200 <= r.status < 400 else 1)
    except Exception:
        time.sleep(1.5)

sys.exit(1)
