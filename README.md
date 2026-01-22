# they own WHAT??

[![Open Source | Love](https://img.shields.io/badge/Open%20Source-Love-red)](https://github.com/sk123/theyownwhat)

**Published at:** [https://theyownwhat.net](https://theyownwhat.net)

**Version 2.0 // 2026**

## Purpose

**they own WHAT??** is an investigative and advocacy tool designed to bring transparency to **Connecticut's** property landscape. By linking fragmented public records, the tool reveals the hidden networks of ownership that shape our neighborhoods.

## Recent Updates (January 2026)

*   **Smart Complex Grouping:** Significantly improved address normalization logic in the Property Table. Identical street addresses (e.g., `195 SIGOURNEY ST`) are now correctly consolidated into complexes even if their sub-unit formats differ.
*   **Refined Expanded Views:** Redesigned the complex unit display with tree-style indentation, blue visual connectors, and clear "Unit X" labeling for better readability.
*   **Enhanced Financial Details:** The property modal now features a robust "Owner Mailing Address" lookup with multiple data fallbacks, ensuring mailing information is visible even when entity links are missing.
*   **Performance & Stability:** Fixed critical geocoding errors (422/500) and optimized the backend to handle large batch requests efficiently using the US Census and Nominatim APIs.
*   **Home Page Polish:** Modernized the hero section with refined font scales and tightened spacing, ensuring "Top Networks" is visible on the first load without scrolling.
*   **AI Digest:** FIXED: Resolved synthesis errors and added inline source citations `(Source: url)` for better verification.

## New Feature: AI Digest

The **AI Digest** performs automated web searches across multiple entities simultaneously to identify:

*   Systemic tenant complaints and property condition issues.
*   Legal violations and court case patterns.
*   Corporate footprints and out-of-state investment trends.
*   Verified news sources and public documentation links.

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

## Contact

Questions? Reach out to [salmunk@gmail.com](mailto:salmunk@gmail.com)

## License

This project is licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License**.

*   **Attribution**: You must give appropriate credit, provide a link to the license, and indicate if changes were made.
*   **Non-Commercial**: You may not use the material for commercial purposes.

See [LICENSE](https://github.com/sk123/theyownwhat/blob/main/LICENSE) for the full text.
