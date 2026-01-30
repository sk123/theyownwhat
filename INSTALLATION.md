# Installation & Setup Guide

This guide explains how to set up the **They Own What** project locally.

## 1. Prerequisites

*   [Docker Desktop](https://www.docker.com/products/docker-desktop) (or Docker Engine + Compose)
*   Git
*   Node.js & npm (for local frontend development)

## 2. Configuration

To enable advanced features like the **Tenant Toolbox** and **AI Digest**, you need to configure your environment variables.

1.  Rename the template file:
    ```bash
    mv ".env (RENAME TO .env)" .env
    ```

2.  Open `.env` and configure the following keys:

    ```env
    # Database Configuration (Default for Docker)
    DATABASE_URL=postgresql://user:password@db:5432/theyownwhat

    # AI Features (Optional - Required for AI Digest)
    OPENAI_API_KEY="sk-..."

    # Tenant Toolbox Features (Optional - Local Only)
    # Set to 'true' to enable the Organizer Dashboard and Group Management
    TOOLBOX_ENABLED=true
    
    # Google Auth (Required if TOOLBOX_ENABLED=true)
    # For local dev, you can use mock auth by setting USE_MOCK_AUTH=true
    GOOGLE_CLIENT_ID="your-google-client-id"
    GOOGLE_CLIENT_SECRET="your-google-client-secret"
    SESSION_SECRET_KEY="change-me-locally"
    USE_MOCK_AUTH=true
    ```

    > [!IMPORTANT]
    > The **Tenant Toolbox** features are currently experimental and designed for local use only. Ensure `TOOLBOX_ENABLED` is set to `true` to access the organizer dashboard at `/toolbox`.

## 3. Data Preparation

The application relies on specific datasets from the State of Connecticut. You must download these files, rename them, and place them in the `data/` directory.

### Step 3.1: Download Data

Download the following datasets (CSV format):

1.  **Businesses**: [CT Business Registry - Business Master](https://data.ct.gov/Business/Connecticut-Business-Registry-Business-Master/n7gp-d28j/about_data)
    *   *Action*: Download as CSV.
    *   *Rename to*: `businesses.csv`

2.  **Principals**: [CT Business Registry - Principals](https://data.ct.gov/Business/Connecticut-Business-Registry-Principals/ka36-64k6/about_data)
    *   *Action*: Download as CSV.
    *   *Rename to*: `principals.csv`

3.  **Properties**: [CT CAMA and Parcel Layer](https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/about)
    *   *Action*: Download as CSV (or export the layer to CSV).
    *   *Rename to*: `new_parcels.csv`

### Step 3.2: Organize Files

Create a `data` folder in the root of the repository if it doesn't exist, and move your renamed files there:

```
tow3/
├── data/
│   ├── businesses.csv
│   ├── principals.csv
│   └── new_parcels.csv
├── docker-compose.yml
└── ...
```

## 4. Deployment

Start the application using Docker Compose. This will build the frontend, backend, and database containers.

```bash
docker compose up -d --build
```

The application should now be running at:
*   **Main Site**: [http://localhost:6262](http://localhost:6262)
*   **Tenant Toolbox**: [http://localhost:6263](http://localhost:6263) (if enabled)

*Note: The first startup may take a minute while the database initializes.*

## 5. Import Data

Once the containers are running, you need to populate the database with the CSV files you downloaded. Run the following commands to execute the import scripts inside the `importer` container context.

**Open a terminal in the project root and run:**

1.  **Import Businesses:**
    ```bash
    docker compose run --rm importer python importer/update_data.py businesses
    ```

2.  **Import Principals:**
    ```bash
    docker compose run --rm importer python importer/update_data.py principals
    ```

3.  **Import Properties:**
    ```bash
    docker compose run --rm importer python importer/update_data.py properties
    ```

*These processes may take several minutes depending on the size of the datasets.*

## 6. Updates & Scrapers

### Scraping Municipal Data
To enrich the database with detailed property information (listing details, zoning, year built, etc.) from municipal sites (VISION, MapXpress), run the updater:

```bash
# Run for all configured municipalities
docker compose run --rm updater python updater/update_data.py

# Run for specific municipalities (e.g., ANSONIA and HARTFORD)
docker compose run --rm updater python updater/update_data.py -m "ANSONIA" "HARTFORD"
```
