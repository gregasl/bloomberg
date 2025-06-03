import os
import uuid
import orjson
import datetime
import time

from pprint import pprint
from urllib.parse import urljoin
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

# Set the following environment variables
catalog = os.environ["BLOOMBERG_DL_ACCOUNT_NUMBER"] # Data License Account Number provided by your account rep/admins
catalog = catalog.removeprefix("DL ")
client_id = os.environ['BLOOMBERG_DL_CLIENT_ID'] # from credential file
client_secret = os.environ['BLOOMBERG_DL_CLIENT_SECRET'] # From credential file

HOST = 'https://api.bloomberg.com'
OAUTH2_ENDPOINT = 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2'

client = BackendApplicationClient(client_id=client_id)

# This SESSION object automatically adds the Authorization header
SESSION = OAuth2Session(client=client, auto_refresh_url=OAUTH2_ENDPOINT,
                        auto_refresh_kwargs={'client_id': client_id},
                        token_updater=lambda x: x)

token = SESSION.fetch_token(token_url=OAUTH2_ENDPOINT, client_secret=client_secret)
request_response_base = f'/eap/catalogs/{catalog}'

identifier = f"IssueDailyInfo{str(uuid.uuid4())[0:4]}" # uuid reduces likelihood of ID overlaps

json_payload = '''{
    "@type": "DataRequest",
    "name": "CusipInfo",
    "identifier": "%s",
    "title": "Get Some Bond Data",
    "universe": {
        "@type": "Universe",
        "contains": [
            {
                "@type": "Identifier",
                "identifierType": "CUSIP",
                "identifierValue": "91282CMV0"
            },
            {
                "@type": "Identifier",
                "identifierType": "CUSIP",
                "identifierValue": "91282CGS4"
            }
        ]
    },
    "fieldList": {
        "@type": "DataFieldList",
        "contains": [
            {
                "mnemonic": "SECURITY_DES"
            },
            {
                "mnemonic": "MATURITY"
            },
            {
                "mnemonic": "ISSUE_DT"
            },
            {
            }
        ]
    },
    "trigger": {
        "@type": "SubmitTrigger"
    },
    "formatting": {
        "@type": "MediaType",
        "outputMediaType": "text/csv"
    }
}
''' % identifier
print("About to make request")
print(f'{json_payload}')

request_uri = urljoin(HOST, request_response_base) + '/requests/'
print(f"request uri {request_uri}")
data = orjson.loads(json_payload)

response = SESSION.post(request_uri, json=data, headers={'api-version': '2'})

print(f"Status Code: {response.status_code}")
pprint(response.json())
snapshot_date = datetime.datetime.now().strftime("%Y%m%d")
content_responses_uri = urljoin(HOST, f'/eap/catalogs/{catalog}/content/responses/?requestIdentifier={identifier}') # identifier comes from the values we used in the POST step above

while True:
    print(f'sending... {content_responses_uri}')
    response = SESSION.get(content_responses_uri, headers={'api-version': '2'})
    if response.status_code == 404:
        print("Response not available. Retry in 15 seconds...")
        time.sleep(15)
        continue

    elif response.status_code == 200:
        response_data = response.json()
        responses = response_data["contains"]
        if len(responses) > 0:
            latest_response = responses[0]
            key = latest_response["key"]
            snapshot_timestamp = latest_response["metadata"]["DL_SNAPSHOT_START_TIME"]
            data_uri = urljoin(HOST, f'/eap/catalogs/{catalog}/content/responses/{key}')
            data_response = SESSION.get(data_uri, headers={'api-version': '2'})
    
            print(f"Latest response found on {data_uri} for snapshot {snapshot_timestamp}. Printing body")
            print(data_response.text)
            break
        
        else:
            print("No generated responses yet. Retrying...")
            time.sleep(60)
            continue
    else:
        print("Unhandled HTTP status code.")
        print(response.status_code)
        print(response.text)
        break
