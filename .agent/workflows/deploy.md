---
description: Steps to deploy the CT Property Network Explorer using Docker.
---

# Deployment Guide

Follow these steps to deploy the application on a new server or update the existing deployment.

### Prerequisites

- Docker and Docker Compose installed.
- `.env` file containing:
  - `OPENAI_API_KEY`
  - `SERPAPI_API_KEY`

### Deployment Steps

1. **Clone the repository** (if not already present):
   ```bash
   git clone <repository-url>
   cd tow3
   ```

// turbo
2. **Build and Start the Containers**:
   Run the following command to build the modern React frontend and start all services in the background:
   ```bash
   docker compose up --build -d
   ```

3. **Verify Deployment**:
   - Access the frontend at `http://localhost:6262`.
   - The API will be available at `http://localhost:8000/api`.

// turbo
4. **Initial Data Import (Optional)**:
   If this is a fresh database, run the importer to populate the property and business data:
   ```bash
   docker compose run --rm importer
   ```

### Troubleshooting

- **Check Logs**:
  ```bash
  docker compose logs -f
  ```
- **Restart Services**:
  ```bash
  docker compose restart api
  ```
- **Database Access**:
  ```bash
  docker exec -it ctdata_db psql -U user -d ctdata
  ```
