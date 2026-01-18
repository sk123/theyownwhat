# Installation & Setup Guide

This guide explains how to set up the **They Own What** project locally.

## 1. Prerequisites

*   [Docker Desktop](https://www.docker.com/products/docker-desktop) (or Docker Engine + Compose)
*   Git

## 2. Data Preparation

The application relies on specific datasets from the State of Connecticut. You must download these files, rename them, and place them in the `data/` directory.

### Step 2.1: Download Data

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

### Step 2.2: Organize Files

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

## 3. Deployment

Start the application using Docker Compose. This will build the frontend, backend, and database containers.

```bash
docker compose up -d --build
```

The application should now be running at [http://localhost:6262](http://localhost:6262).

*Note: The first startup may take a minute while the database initializes.*

## 4. Import Data

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

## 5. Updates

To update the data in the future, simply replace the CSV files in the `data/` directory with fresh downloads (renaming them as before) and re-run the commands in Step 4.
