"""Massachusetts municipal CAMA & open-assessor data source configuration.

Shared between `ma/scrape_ma_cama.py`, `api/main.py` (`/api/completeness`),
and `updater/scheduled_runner.py`.

Covers 112 verified live Massachusetts municipal CAMA / open-assessor portals:
  - 78 Vision Government Solutions (`gis.vgsi.com`) CAMA portals
  - 31 MapGeo (`*.mapgeo.io`) municipal CAMA portals
  - 1 City of Boston Assessing (`data.boston.gov` / `properties.boston.gov`)
  - 1 City of Cambridge Assessing (`data.cambridgema.gov` FY2027 Property Database)
  - 1 ActDataScout (`actdatascout.com` — Ashland)

The remaining 239 of 351 Massachusetts municipalities are standardized via
MassGIS Level 3 Property Tax Parcels (`Massachusetts_Property_Tax_Parcels`)
after annual MA Department of Revenue (DOR) tax-roll certification.
"""

MA_MUNICIPAL_DATA_SOURCES = {
    "ABINGTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/abingtonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/abingtonma/",
        "vgsi_listed_date": "12/29/2025"
    },
    "ACTON": {
        "domain": "actonma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://actonma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://actonma.mapgeo.io/"
    },
    "AGAWAM": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/agawamma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/agawamma/",
        "vgsi_listed_date": "12/22/2025"
    },
    "AMESBURY": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/amesburyma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/amesburyma/",
        "vgsi_listed_date": "12/11/2024"
    },
    "ASHLAND": {
        "frequency": "Vision CAMA",
        "portal_url": "https://www.actdatascout.com/RealProperty/Massachusetts/Ashland",
        "type": "actdatascout",
        "url": "https://www.actdatascout.com/RealProperty/Massachusetts/Ashland",
        "vgsi_listed_date": "05/01/2026"
    },
    "AUBURN": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/AuburnMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/AuburnMA/",
        "vgsi_listed_date": "12/05/2024"
    },
    "AVON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/AvonMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/AvonMA/",
        "vgsi_listed_date": "01/24/2025"
    },
    "BELMONT": {
        "domain": "belmontma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://belmontma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://belmontma.mapgeo.io/"
    },
    "BERKLEY": {
        "domain": "berkleyma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://berkleyma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://berkleyma.mapgeo.io/"
    },
    "BERLIN": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/BerlinMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/BerlinMA/",
        "vgsi_listed_date": "11/16/2026"
    },
    "BEVERLY": {
        "domain": "beverlyma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://beverlyma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://beverlyma.mapgeo.io/"
    },
    "BOSTON": {
        "frequency": "Weekly",
        "portal_url": "https://properties.boston.gov/",
        "type": "boston_assessing",
        "url": "https://data.boston.gov/dataset/property-assessment"
    },
    "BOXFORD": {
        "frequency": "Nightly",
        "portal_url": "https://gis.vgsi.com/BoxfordMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/BoxfordMA/",
        "vgsi_listed_date": "Nightly"
    },
    "BURLINGTON": {
        "domain": "burlingtonma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://burlingtonma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://burlingtonma.mapgeo.io/"
    },
    "CAMBRIDGE": {
        "frequency": "Weekly",
        "portal_url": "https://www.cambridgema.gov/PropertyDatabase",
        "type": "cambridge_socrata",
        "url": "https://data.cambridgema.gov/resource/eey2-rv59.json"
    },
    "CHELMSFORD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/chelmsfordma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/chelmsfordma/",
        "vgsi_listed_date": "01/14/2026"
    },
    "CHELSEA": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/chelseama/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/chelseama/",
        "vgsi_listed_date": "01/10/2025"
    },
    "CHICOPEE": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/chicopeema/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/chicopeema/",
        "vgsi_listed_date": "11/20/2025"
    },
    "CLINTON": {
        "frequency": "Nightly",
        "portal_url": "https://gis.vgsi.com/clintonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/clintonma/",
        "vgsi_listed_date": "Nightly"
    },
    "CONCORD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/concordma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/concordma/",
        "vgsi_listed_date": "10/07/2026"
    },
    "DARTMOUTH": {
        "frequency": "Monthly",
        "portal_url": "https://gis.vgsi.com/dartmouthma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/dartmouthma/",
        "vgsi_listed_date": "Monthly"
    },
    "DEDHAM": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/dedhamma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/dedhamma/",
        "vgsi_listed_date": "04/03/2026"
    },
    "DRACUT": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/dracutma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/dracutma/",
        "vgsi_listed_date": "01/08/2026"
    },
    "DUDLEY": {
        "frequency": "Nightly",
        "portal_url": "https://gis.vgsi.com/dudleyma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/dudleyma/",
        "vgsi_listed_date": "Nightly"
    },
    "DUNSTABLE": {
        "domain": "dunstablema.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://dunstablema.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://dunstablema.mapgeo.io/"
    },
    "EAST BROOKFIELD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/EastBrookfieldMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/EastBrookfieldMA/",
        "vgsi_listed_date": "08/03/2026"
    },
    "FALMOUTH": {
        "domain": "falmouthma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://falmouthma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://falmouthma.mapgeo.io/"
    },
    "FRAMINGHAM": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/framinghamma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/framinghamma/",
        "vgsi_listed_date": "12/29/2025"
    },
    "GARDNER": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/gardnerma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/gardnerma/",
        "vgsi_listed_date": "01/09/2026"
    },
    "GLOUCESTER": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/GloucesterMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/GloucesterMA/",
        "vgsi_listed_date": "12/06/2023"
    },
    "GRAFTON": {
        "frequency": "Monthly",
        "portal_url": "https://gis.vgsi.com/graftonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/graftonma/",
        "vgsi_listed_date": "Monthly"
    },
    "GROTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/grotonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/grotonma/",
        "vgsi_listed_date": "01/03/2023"
    },
    "HAMPDEN": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/hampdenma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/hampdenma/",
        "vgsi_listed_date": "12/26/2024"
    },
    "HANOVER": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/hanoverma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/hanoverma/",
        "vgsi_listed_date": "12/31/2024"
    },
    "HAVERHILL": {
        "domain": "haverhillma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://haverhillma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://haverhillma.mapgeo.io/"
    },
    "HINGHAM": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/hinghamma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/hinghamma/",
        "vgsi_listed_date": "12/23/2024"
    },
    "HOLDEN": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/holdenma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/holdenma/",
        "vgsi_listed_date": "12/18/2024"
    },
    "HOLLAND": {
        "frequency": "Monthly",
        "portal_url": "https://gis.vgsi.com/HollandMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/HollandMA/",
        "vgsi_listed_date": "Monthly"
    },
    "HOLYOKE": {
        "domain": "holyokema.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://holyokema.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://holyokema.mapgeo.io/"
    },
    "HUDSON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/hudsonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/hudsonma/",
        "vgsi_listed_date": "01/06/2026"
    },
    "IPSWICH": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/ipswichma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/ipswichma/",
        "vgsi_listed_date": "12/24/2025"
    },
    "LAWRENCE": {
        "frequency": "Weekly",
        "portal_url": "https://gis.vgsi.com/lawrencema/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/lawrencema/",
        "vgsi_listed_date": "Weekly"
    },
    "LEXINGTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/lexingtonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/lexingtonma/",
        "vgsi_listed_date": "12/28/2025"
    },
    "LONGMEADOW": {
        "frequency": "Monthly",
        "portal_url": "https://gis.vgsi.com/longmeadowma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/longmeadowma/",
        "vgsi_listed_date": "Monthly"
    },
    "LOWELL": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/lowellma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/lowellma/",
        "vgsi_listed_date": "12/01/2025"
    },
    "MALDEN": {
        "domain": "maldenma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://maldenma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://maldenma.mapgeo.io/"
    },
    "MARION": {
        "domain": "marionma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://marionma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://marionma.mapgeo.io/"
    },
    "MATTAPOISETT": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/mattapoisettma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/mattapoisettma/",
        "vgsi_listed_date": "01/14/2025"
    },
    "MEDFORD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/medfordma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/medfordma/",
        "vgsi_listed_date": "12/08/2020"
    },
    "MIDDLEBOROUGH": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/MiddleboroughMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/MiddleboroughMA/",
        "vgsi_listed_date": "12/12/2023"
    },
    "MILFORD": {
        "domain": "milfordma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://milfordma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://milfordma.mapgeo.io/"
    },
    "MILLBURY": {
        "frequency": "Monthly",
        "portal_url": "https://gis.vgsi.com/millburyma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/millburyma/",
        "vgsi_listed_date": "Monthly"
    },
    "NANTUCKET": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/nantucketma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/nantucketma/",
        "vgsi_listed_date": "12/19/2025"
    },
    "NATICK": {
        "domain": "natickma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://natickma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://natickma.mapgeo.io/"
    },
    "NEWBURYPORT": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/newburyportma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/newburyportma/",
        "vgsi_listed_date": "12/05/2024"
    },
    "NEWTON": {
        "domain": "newtonma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://newtonma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://newtonma.mapgeo.io/"
    },
    "NORFOLK": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/norfolkma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/norfolkma/",
        "vgsi_listed_date": "12/24/2025"
    },
    "NORTH ANDOVER": {
        "domain": "northandoverma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://northandoverma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://northandoverma.mapgeo.io/"
    },
    "NORTH ATTLEBOROUGH": {
        "frequency": "Monthly",
        "portal_url": "https://gis.vgsi.com/northattleboroma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/northattleboroma/",
        "vgsi_listed_date": "Monthly"
    },
    "NORTHBOROUGH": {
        "domain": "northboroughma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://northboroughma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://northboroughma.mapgeo.io/"
    },
    "NORTHBRIDGE": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/NorthbridgeMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/NorthbridgeMA/",
        "vgsi_listed_date": "02/04/2026"
    },
    "NORTON": {
        "domain": "nortonma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://nortonma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://nortonma.mapgeo.io/"
    },
    "NORWELL": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/NorwellMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/NorwellMA/",
        "vgsi_listed_date": "12/23/2025"
    },
    "NORWOOD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/norwoodma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/norwoodma/",
        "vgsi_listed_date": "12/28/2023"
    },
    "PALMER": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/palmerma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/palmerma/",
        "vgsi_listed_date": "01/09/2025"
    },
    "PAXTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/PaxtonMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/PaxtonMA/",
        "vgsi_listed_date": "02/03/2026"
    },
    "PEABODY": {
        "domain": "peabodyma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://peabodyma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://peabodyma.mapgeo.io/"
    },
    "PITTSFIELD": {
        "domain": "pittsfieldma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://pittsfieldma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://pittsfieldma.mapgeo.io/"
    },
    "PLYMPTON": {
        "domain": "plymptonma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://plymptonma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://plymptonma.mapgeo.io/"
    },
    "PRINCETON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/PrincetonMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/PrincetonMA/",
        "vgsi_listed_date": "02/25/2022"
    },
    "QUINCY": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/quincyma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/quincyma/",
        "vgsi_listed_date": "12/16/2025"
    },
    "RANDOLPH": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/RandolphMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/RandolphMA/",
        "vgsi_listed_date": "01/22/2025"
    },
    "RAYNHAM": {
        "domain": "raynhamma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://raynhamma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://raynhamma.mapgeo.io/"
    },
    "READING": {
        "domain": "readingma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://readingma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://readingma.mapgeo.io/"
    },
    "REHOBOTH": {
        "domain": "rehobothma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://rehobothma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://rehobothma.mapgeo.io/"
    },
    "ROCHESTER": {
        "domain": "rochesterma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://rochesterma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://rochesterma.mapgeo.io/"
    },
    "ROWLEY": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/RowleyMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/RowleyMA/",
        "vgsi_listed_date": "08/31/2026"
    },
    "SANDWICH": {
        "domain": "sandwichma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://sandwichma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://sandwichma.mapgeo.io/"
    },
    "SEEKONK": {
        "domain": "seekonkma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://seekonkma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://seekonkma.mapgeo.io/"
    },
    "SHARON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/SharonMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/SharonMA/",
        "vgsi_listed_date": "08/12/2026"
    },
    "SHREWSBURY": {
        "frequency": "Weekly",
        "portal_url": "https://gis.vgsi.com/ShrewsburyMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/ShrewsburyMA/",
        "vgsi_listed_date": "Weekly"
    },
    "SOMERVILLE": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/SomervilleMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/SomervilleMA/",
        "vgsi_listed_date": "12/20/2024"
    },
    "SOUTHAMPTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/southamptonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/southamptonma/",
        "vgsi_listed_date": "12/08/2025"
    },
    "SOUTHBRIDGE": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/southbridgema/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/southbridgema/",
        "vgsi_listed_date": "10/31/2024"
    },
    "SOUTHWICK": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/southwickma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/southwickma/",
        "vgsi_listed_date": "12/16/2024"
    },
    "STOW": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/stowma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/stowma/",
        "vgsi_listed_date": "01/12/2026"
    },
    "STURBRIDGE": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/sturbridgema/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/sturbridgema/",
        "vgsi_listed_date": "09/23/2024"
    },
    "SUTTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/suttonMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/suttonMA/",
        "vgsi_listed_date": "11/20/2025"
    },
    "SWANSEA": {
        "frequency": "Nightly",
        "portal_url": "https://gis.vgsi.com/swanseaMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/swanseaMA/",
        "vgsi_listed_date": "Nightly"
    },
    "TAUNTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/tauntonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/tauntonma/",
        "vgsi_listed_date": "01/08/2026"
    },
    "TEWKSBURY": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/TewksburyMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/TewksburyMA/",
        "vgsi_listed_date": "12/19/2024"
    },
    "TYNGSBOROUGH": {
        "domain": "tyngsboroughma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://tyngsboroughma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://tyngsboroughma.mapgeo.io/"
    },
    "WAKEFIELD": {
        "domain": "wakefieldma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://wakefieldma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://wakefieldma.mapgeo.io/"
    },
    "WALES": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/walesma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/walesma/",
        "vgsi_listed_date": "01/20/2026"
    },
    "WALPOLE": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/walpolema/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/walpolema/",
        "vgsi_listed_date": "10/16/2026"
    },
    "WALTHAM": {
        "domain": "walthamma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://walthamma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://walthamma.mapgeo.io/"
    },
    "WAREHAM": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/warehamma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/warehamma/",
        "vgsi_listed_date": "05/11/2026"
    },
    "WATERTOWN": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/WatertownMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/WatertownMA/",
        "vgsi_listed_date": "12/29/2025"
    },
    "WAYLAND": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/WaylandMA/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/WaylandMA/",
        "vgsi_listed_date": "12/11/2024"
    },
    "WEST BROOKFIELD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/westbrookfieldma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/westbrookfieldma/",
        "vgsi_listed_date": "01/13/2025"
    },
    "WEST NEWBURY": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/westnewburyma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/westnewburyma/",
        "vgsi_listed_date": "06/04/2025"
    },
    "WESTFIELD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/westfieldma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/westfieldma/",
        "vgsi_listed_date": "11/22/2023"
    },
    "WESTHAMPTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/westhamptonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/westhamptonma/",
        "vgsi_listed_date": "01/12/2023"
    },
    "WESTPORT": {
        "domain": "westportma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://westportma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://westportma.mapgeo.io/"
    },
    "WESTWOOD": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/westwoodma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/westwoodma/",
        "vgsi_listed_date": "11/21/2025"
    },
    "WEYMOUTH": {
        "domain": "weymouthma.mapgeo.io",
        "frequency": "Weekly",
        "portal_url": "https://weymouthma.mapgeo.io/",
        "type": "mapgeo",
        "url": "https://weymouthma.mapgeo.io/"
    },
    "WILMINGTON": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/wilmingtonma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/wilmingtonma/",
        "vgsi_listed_date": "12/31/2025"
    },
    "WINTHROP": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/winthropma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/winthropma/",
        "vgsi_listed_date": "01/05/2026"
    },
    "WOBURN": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/woburnma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/woburnma/",
        "vgsi_listed_date": "12/20/2024"
    },
    "WORCESTER": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/worcesterma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/worcesterma/",
        "vgsi_listed_date": "10/23/2026"
    },
    "WRENTHAM": {
        "frequency": "Vision CAMA",
        "portal_url": "https://gis.vgsi.com/wrenthamma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/wrenthamma/",
        "vgsi_listed_date": "12/30/2025"
    },
    "YARMOUTH": {
        "frequency": "Nightly",
        "portal_url": "https://gis.vgsi.com/yarmouthma/",
        "type": "vision_appraisal",
        "url": "https://gis.vgsi.com/yarmouthma/",
        "vgsi_listed_date": "Nightly"
    }
}

def ma_muni_status_key(town_name: str) -> str:
    """Return collision-free `data_source_status.source_name` for an MA municipality."""
    return f"MA_MUNI_{(town_name or "").strip().upper()}"
