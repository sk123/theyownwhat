---
description: Rebuild and restart the application stack to apply frontend/backend changes.
---

1. Stop and remove the old containers for the core services.
```bash
docker compose stop nginx api updater
docker compose rm -f nginx api updater
```

2. Rebuild the images to pick up code changes (especially in the frontend).
// turbo
3. Start the services in detached mode.
```bash
docker compose up -d --build nginx api updater
```

4. Verify the frontend is healthy.
```bash
docker compose ps
```
