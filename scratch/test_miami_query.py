import requests

url = "https://gisweb.miamidade.gov/arcgis/rest/services/MD_LandInformation/MapServer/26/query"
params = {
    'where': 'TRUE_OWNER1 IS NOT NULL AND TRUE_SITE_ADDR IS NOT NULL',
    'outFields': 'FOLIO,TRUE_SITE_ADDR,TRUE_SITE_CITY,TRUE_SITE_ZIP_CODE,TRUE_OWNER1,TRUE_OWNER2,TRUE_MAILING_ADDR1,TRUE_MAILING_ADDR2,TRUE_MAILING_CITY,TRUE_MAILING_STATE,TRUE_MAILING_ZIP_CODE,YEAR_BUILT,TOTAL_VAL_CUR,UNIT_COUNT',
    'resultRecordCount': 3,
    'f': 'json'
}
r = requests.get(url, params=params)
data = r.json()
features = data.get('features', [])
for f in features:
    print(f.get('attributes'))
