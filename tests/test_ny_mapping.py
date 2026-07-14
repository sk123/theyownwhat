import pandas as pd
import unittest

class TestStateDataMappings(unittest.TestCase):
    def test_ct_and_ny_business_mappings(self):
        # 1. CT columns format
        ct_columns = {
            'id': '100', 'name': 'CT PROPERTY HOLDINGS LLC', 'status': 'active', 
            'date_of_formation': '2020-01-01', 'business_type': 'LLC', 
            'principal_name': 'JOHN SMITH', 'business_address': '123 MAIN ST', 
            'business_city': 'HARTFORD', 'business_state': 'CT', 'business_zip': '06103'
        }
        
        # 2. NY columns format
        ny_columns = {
            'dos_id': '200', 'current_entity_name': 'NY PROPERTY HOLDINGS LLC', 'entity_status': 'active',
            'initial_dos_filing_date': '2021-02-02', 'entity_type': 'LLC',
            'dos_process_name': 'JANE DOE', 'dos_process_address': '456 BROADWAY',
            'dos_process_city': 'ALBANY', 'dos_process_state': 'NY', 'dos_process_zip_code': '12207'
        }
        
        # Mappings defined in import_data.py
        business_cols = {
            'id': 'id', 'dos_id': 'id', 'name': 'name', 'current_entity_name': 'name', 'status': 'status', 'entity_status': 'status',
            'date_of_formation': 'date_of_formation',
            'date_of_organization_meeting': 'date_of_formation',
            'initial_dos_filing_date': 'date_of_formation',
            'business_type': 'business_type', 'entity_type': 'business_type', 'nature_of_business': 'nature_of_business', 
            'principal_name': 'principal_name', 'agent_name': 'principal_name',
            'agent': 'principal_name', 'registered_agent': 'principal_name',
            'registered_agent_name': 'principal_name', 'dos_process_name': 'principal_name',
            'principal_address': 'principal_address', 'agent_address': 'principal_address',
            'agent_street_address': 'principal_address',
            'business_address': 'business_address',
            'business_street_address_1': 'business_address',
            'street_address': 'business_address',
            'address1': 'business_address',
            'address_line_1': 'business_address',
            'street': 'business_address',
            'business_street': 'business_address',
            'dos_process_address_line_1': 'business_address',
            'dos_process_address': 'business_address',
            'business_city': 'business_city', 'dos_process_city': 'business_city',
            'business_state': 'business_state', 'dos_process_state': 'business_state',
            'business_zip': 'business_zip', 'dos_process_zip_code': 'business_zip'
        }
        
        # Map CT Data
        df_ct = pd.DataFrame([ct_columns])
        df_ct.rename(columns=business_cols, inplace=True)
        self.assertEqual(df_ct.loc[0, 'id'], '100')
        self.assertEqual(df_ct.loc[0, 'name'], 'CT PROPERTY HOLDINGS LLC')
        self.assertEqual(df_ct.loc[0, 'business_address'], '123 MAIN ST')
        self.assertEqual(df_ct.loc[0, 'business_state'], 'CT')
        
        # Map NY Data
        df_ny = pd.DataFrame([ny_columns])
        df_ny.rename(columns=business_cols, inplace=True)
        self.assertEqual(df_ny.loc[0, 'id'], '200')
        self.assertEqual(df_ny.loc[0, 'name'], 'NY PROPERTY HOLDINGS LLC')
        self.assertEqual(df_ny.loc[0, 'business_address'], '456 BROADWAY')
        self.assertEqual(df_ny.loc[0, 'business_state'], 'NY')

    def test_ct_and_ny_property_mappings(self):
        # 1. CT columns format
        ct_columns = {
            'pid': 'CT-999', 'owner': 'CT LANDLORD INC', 'co_owner': 'CT CO-OWNER INC',
            'location_cama': '789 MAPLE AVE', 'style_desc': 'APARTMENTS', 'living_area': 1200,
            'ayb': 1990, 'land_acres': 1.5, 'assessed_total': 150000, 'appraised_total': 210000
        }
        
        # 2. NY columns format
        ny_columns = {
            'print_key': 'NY-888', 'primary_owner': 'NY LANDLORD INC', 'add_owner': 'NY CO-OWNER INC',
            'parcel_addr': '456 ELM ST', 'prop_class': 'APARTMENTS', 'sqft_living': 1500,
            'yr_blt': 2005, 'acres': 2.0, 'total_av': 180000, 'full_market_val': 250000
        }
        
        property_cols = {
            'pid': 'serial_number', 'print_key': 'serial_number', 'sbl': 'serial_number',
            'list_year': 'list_year',
            'property_city': 'property_city', 'muni_name': 'property_city',
            'owner': 'owner', 'primary_owner': 'owner',
            'co_owner': 'co_owner', 'add_owner': 'co_owner',
            'location_cama': 'location', 'parcel_addr': 'location',
            'style_desc': 'property_type', 'prop_class': 'property_type',
            'living_area': 'living_area', 'sqft_living': 'living_area',
            'ayb': 'year_built', 'yr_blt': 'year_built',
            'land_acres': 'acres', 'acres': 'acres',
            'assessed_total': 'assessed_value', 'total_av': 'assessed_value',
            'appraised_total': 'appraised_value', 'full_market_val': 'appraised_value'
        }
        
        # Map CT Data
        df_ct = pd.DataFrame([ct_columns])
        df_ct.rename(columns=property_cols, inplace=True)
        self.assertEqual(df_ct.loc[0, 'serial_number'], 'CT-999')
        self.assertEqual(df_ct.loc[0, 'owner'], 'CT LANDLORD INC')
        self.assertEqual(df_ct.loc[0, 'location'], '789 MAPLE AVE')
        self.assertEqual(df_ct.loc[0, 'year_built'], 1990)
        
        # Map NY Data
        df_ny = pd.DataFrame([ny_columns])
        df_ny.rename(columns=property_cols, inplace=True)
        self.assertEqual(df_ny.loc[0, 'serial_number'], 'NY-888')
        self.assertEqual(df_ny.loc[0, 'owner'], 'NY LANDLORD INC')
        self.assertEqual(df_ny.loc[0, 'location'], '456 ELM ST')
        self.assertEqual(df_ny.loc[0, 'year_built'], 2005)

if __name__ == '__main__':
    unittest.main()
