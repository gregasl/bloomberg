import os
import io
import datetime
import uuid
from  urllib.parse import urljoin

import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

catalog = os.environ["BLOOMBERG_DL_ACCOUNT_NUMBER"] # Data License Account Number provided by your account rep/admins
client_id = os.environ['BLOOMBERG_DL_CLIENT_ID'] # From credential file
client_secret = os.environ['BLOOMBERG_DL_CLIENT_SECRET'] # From credential file

HOST = 'https://api.bloomberg.com'
OAUTH2_ENDPOINT = 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2'

client = BackendApplicationClient(client_id=client_id)

# This SESSION object automatically adds the Authorization header
SESSION = OAuth2Session(client=client, auto_refresh_url=OAUTH2_ENDPOINT,
                        auto_refresh_kwargs={'client_id': client_id},
                        token_updater=lambda x: x)

token = SESSION.fetch_token(token_url=OAUTH2_ENDPOINT, client_secret=client_secret)


catalogs_url = urljoin(HOST, '/eap/catalogs/bbg')
headers = {'api-version':'2'}
response = SESSION.get(catalogs_url, headers=headers)

print(f"Status Code: {response.status_code}")
print(response.json())