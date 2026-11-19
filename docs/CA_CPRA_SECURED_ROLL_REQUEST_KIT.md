# California County Assessor CPRA Secured Assessment Roll Request Kit (Track B)

## Statutory Context & Why Track B Exists

Under **California Government Code § 7928.205 (AB 1785)**, California state and local agencies are prohibited from posting the home address or **Name + Assessor Parcel Number (APN)** of elected/appointed officials, judges, and peace officers on the unrestricted public internet without written permission. Because county assessors do not maintain an automated real-time filter for all protected officials across unrestricted GIS feeds, **nearly all 58 California County Assessors strip `Owner Name` from their free online GIS downloads**.

However, under **California Revenue & Taxation Code § 408.3** and the **California Public Records Act (CPRA, Gov. Code § 7920.000 et seq. & § 7922.575)**, the **Secured Assessment Roll** (containing `APN/AIN`, `Assessee / Deed Owner Name`, `Mailing Address`, `Situs Address`, `Use Code`, `Units`, `Year Built`, `Land Value`, and `Improvement Value`, with verified § 7928.205 confidential-official records redacted by the county) **is an open public record obtainable on electronic media / SFTP at statutory duplication cost**.

Once any county's Secured Assessment Roll CSV/TXT file is obtained, load it directly into `ca_properties` and re-link against our **9.42M California corporations/LLCs & 31.96M LLC managers/members (`us_ca`)** with a single command:

```bash
systemd-run --user --scope -p MemoryHigh=14G -p MemoryMax=18G \
  python3 ca/ingest_ca_statewide.py \
    --import-cpra-roll "ALAMEDA" /path/to/alameda_secured_roll.csv \
    --rebuild-networks
```

---

## Top 12 Priority Counties (~78% of California's Population)

| Priority | County | Metro Covered | Assessor Portal / Data Request Contact | Published Roll Notes |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Los Angeles** | Los Angeles, Long Beach, Santa Monica, Pasadena, Glendale (`2.4M` AINs) | https://assessor.lacounty.gov/real-estate-toolkit/data-sales (`datasales@assessor.lacounty.gov`) | Secured Basic File (`DS04`) + Sales List delivered electronically / SFTP |
| **2** | **Alameda** | Oakland, Berkeley, Fremont, Hayward | https://www.acassessor.org/ (`assessor@acgov.org`) | Published fee: **`$20`** for complete Secured & Unsecured Assessment Roll CD/electronic file |
| **3** | **San Francisco** | City & County of San Francisco | https://sfassessor.org/ (`assessor@sfgov.org` / NextRequest CPRA) | Secured Roll electronic extract under Gov. Code § 7922.575 |
| **4** | **San Diego** | San Diego, Chula Vista, Oceanside | https://www.sdarcc.gov/ (CPRA Officer) | **Important:** Request strictly as a **CPRA electronic Secured Assessment Roll extract** under Gov. Code § 7922.575 (avoid counter parcel-characteristic per-parcel fee schedule) |
| **5** | **Orange** | Anaheim, Santa Ana, Irvine, Huntington Beach | https://www.ocassessor.gov/ | Secured Assessment Roll electronic file |
| **6** | **Santa Clara** | San Jose, Sunnyvale, Mountain View, Palo Alto | https://www.sccassessor.org/ | Secured Assessment Roll electronic extract |
| **7** | **Sacramento** | Sacramento, Elk Grove, Citrus Heights | https://assessor.saccounty.gov/ | Secured Assessment Roll electronic extract |
| **8** | **Riverside** | Riverside, Moreno Valley, Corona | https://www.asrclkrec.com/ | Secured Roll Data Extract |
| **9** | **San Bernardino** | San Bernardino, Fontana, Ontario | https://arc.sbcounty.gov/ | Secured Roll Data Extract |
| **10** | **Contra Costa** | Richmond, Concord, Walnut Creek, Antioch | https://www.contracosta.ca.gov/191/Assessor | Secured Assessment Roll file |
| **11** | **San Mateo** | Daly City, Redwood City, San Mateo, East Palo Alto | https://www.smcacre.gov/assessor | Secured Assessment Roll file |
| **12** | **Fresno** | Fresno, Clovis | https://www.fresnocountyca.gov/Departments/Assessor | Secured Assessment Roll file |

---

## Ready-to-Send CPRA Template Letter (Preparation Draft — Review & Fill Brackets Before Sending)

> **Note:** Review bracketed fields (`[YOUR NAME]`, `[ORGANIZATION / PROJECT]`, `[COUNTY NAME]`, `[DATE]`) before sending via email or the county's NextRequest CPRA portal.

```text
Subject: California Public Records Act Request — Current Electronic Secured Assessment Roll ([COUNTY NAME] County)

Dear Custodian of Records, Office of the [COUNTY NAME] County Assessor:

Pursuant to the California Public Records Act (Cal. Gov. Code § 7920.000 et seq., including § 7922.570–7922.580 governing electronic records) and Cal. Rev. & Tax. Code § 408.3, I respectfully request an electronic copy (CSV, pipe-delimited, or fixed-width text via download link, SFTP, or electronic media) of the current [COUNTY NAME] County Secured Property Tax Assessment Roll containing non-confidential fields maintained in the roll database, specifically:

  1. Assessor Parcel Number (APN / AIN) and Tax Rate Area (TRA)
  2. Assessee / Owner of Record Name(s) and Care-Of Name (excluding any records statutorily confidential under Cal. Gov. Code § 7928.205)
  3. Assessee Mailing Street Address, City, State, and ZIP Code
  4. Property Situs Street Address, Unit, City, and ZIP Code
  5. Use Code / Property Class Description, Number of Residential Units, Stories, and Year Built (where present on the roll)
  6. Assessed Land Value, Assessed Improvement/Structure Value, Exemptions, Net Assessed Value, and Most Recent Recorded Transfer/Document Date

Attestation of Public-Interest Research Purpose:
This request is made solely for non-commercial public-interest housing and property ownership research. The records will not be used directly or indirectly to sell a product or service or for commercial solicitation.

Under Gov. Code § 7922.575(a), please advise of any direct cost of duplicating the existing electronic file prior to fulfillment if that cost exceeds $[50.00]. Please deliver the electronic file or download instructions to [YOUR EMAIL ADDRESS].

Thank you for your assistance.

Sincerely,
[YOUR NAME]
[ORGANIZATION / PROJECT]
[YOUR EMAIL ADDRESS]
[YOUR PHONE / MAILING ADDRESS]
```
