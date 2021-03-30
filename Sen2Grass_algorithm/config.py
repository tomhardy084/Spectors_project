# ------------------------------------------------------------------------------
# NEXUS CONFIG
# ------------------------------------------------------------------------------
import os

# Enter your Nexus username and password below 
DB_USER = '<user_name>'
DB_PASSWORD = '<password>'

# Enter you Nexus access key ID and Secret access key
ACCESS_KEY_ID = '<access_key_id>'
SECRET_ACCESS_KEY = '<secret_access_key>'

# ------------------------------------------------------------------------------
# Constants below should not be modified,
# unless you are sure what you're doing
# ------------------------------------------------------------------------------
BUCKET = 'stsnexus'

DATABASE = {
    'HOST':     '<database_host>',
    'NAME':     'geodata_cache',
    'PORT':     '5432',
    'USER':     os.environ.get('DB_USER', DB_USER),
    'PASSWORD': os.environ.get('DB_PASSWORD', DB_PASSWORD)
    }

DEBUG = os.environ.get('DEBUG', 'True') == 'True'

ACCESS_KEY_ID = os.environ.get('ACCESS_KEY_ID', ACCESS_KEY_ID) 
SECRET_ACCESS_KEY = os.environ.get('SECRET_ACCESS_KEY', SECRET_ACCESS_KEY)