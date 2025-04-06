"""
Field mapping configurations for data transformation
"""

# Organization field mappings between API (key) and CSV (value) data
ORGANIZATION_FIELD_MAPPINGS = {
    'state': 'region',
    'country': 'country_code',
    'street_address': 'address',
    'website_url': 'homepage_url',
}

# People field mappings between API and CSV data
PEOPLE_FIELD_MAPPINGS = {
    'state': 'reigon',
    'country': 'country_code',
}