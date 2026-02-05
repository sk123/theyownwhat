# they own WHAT??

[![Open Source | Love](https://img.shields.io/badge/Open%20Source-Love-red)](https://github.com/sk123/theyownwhat)
[![Sponsor](https://img.shields.io/badge/Sponsor-Pink?style=flat&logo=github-sponsors&logoColor=white)](https://github.com/sponsors/sk123)

**Source Code:** [https://github.com/sk123/theyownwhat](https://github.com/sk123/theyownwhat)

**Published at:** [https://theyownwhat.net](https://theyownwhat.net)

**Version 2.0 // 2026**

## Purpose

**they own WHAT??** is an investigative and advocacy tool designed to bring transparency to **Connecticut's** property landscape. By linking fragmented public records, the tool reveals the hidden networks of ownership that shape neighborhoods.

## Recent Updates (January 2026)

*   **Subsidized Housing Integration:** Integrated the **National Housing Preservation Database (NHPD)**. The system flags properties with active subsidies (Section 8, LIHTC, etc.) and displays program info in the property modal.
*   **Search Interface:** Merged the search bar and button into a unified interface with autocomplete and "No results" messaging.
*   **Complex Grouping:** Updated address normalization logic in the Property Table.
*   **Performance & Stability:** Fixed geocoding errors and updated the backend to handle batch requests.
*   **Home Page:** Updated the hero section and the "Top Networks" display.

## Key Features

### 🕸️ Network Discovery
The "Top Networks" engine visualizes connections between shell companies. It supports:
*   **Graph Visualization:** Displays business and principal connections.
*   **Portfolio Analysis:** Aggregates total assessed value and property counts for discovered networks.

### 🤖 AI Digest
The **AI Digest** performs automated web searches across multiple entities simultaneously to identify:
*   Systemic tenant complaints and property condition issues.
*   Legal violations and court case patterns.
*   Corporate footprints and out-of-state investment trends.



## Data Sources

*   [CT Business Registry - Business Master](https://data.ct.gov/Business/Connecticut-Business-Registry-Business-Master/n7gp-d28j/about_data)
*   [CT Business Registry - Principals](https://data.ct.gov/Business/Connecticut-Business-Registry-Principals/ka36-64k6/about_data)
*   [Municipal Parcel & CAMA Records](https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/about)
*   Fresh & supplemental data scraped from VISION, MapXpress, and other municipal GIS sites
*   Real-time News Highlights

## How it works

The system uses name normalization and link-analysis to connect principals to businesses, and businesses to properties, creating a "graph" of ownership that surpasses simple database lookups.

## Installation & Setup

Want to run this locally? See the [Installation Guide](https://github.com/sk123/theyownwhat/blob/main/INSTALLATION.md) for detailed instructions on how to download the required data and run the Docker containers.

## Transparency Notice

This tool is for informational and advocacy purposes. While every effort is made for 100% accuracy in the linking logic, users should verify critical findings with primary municipal and state sources.

## Contact & Support

> [!NOTE]
> **they own WHAT??** is currently in active development. We are still ironing out the kinks in the network discovery and property linking logic. If you encounter incorrect data or "meganetworks," please let us know!

Feel free to report issues via **GitHub Issues** or reach out to [salmunk@gmail.com](mailto:salmunk@gmail.com).

> "I will do this work regardless of whether I'm paid or not. However, the more funding Dr. Benjamin provides, the more time I can dedicate to this project instead of taking on solely-for-money jobs."

## License

This project is licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License**.

*   **Attribution**: You must give appropriate credit, provide a link to the license, and indicate if changes were made.
*   **Non-Commercial**: You may not use the material for commercial purposes.

See [LICENSE](https://github.com/sk123/theyownwhat/blob/main/LICENSE) for the full text.
