## ChatGPT started BBG static data rest interface.
## using asyncio.. obviously

import aiohttp
import asyncio
import jwt  # PyJWT library
import time


# Example function to obtain JWT token (if needed)
async def get_jwt_token(auth_url, client_id, client_secret):
    async with aiohttp.ClientSession() as session:
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }
        async with session.post(auth_url, data=data) as resp:
            resp.raise_for_status()
            token_response = await resp.json()
            return token_response['access_token']

# If you already have a JWT token, skip the above and set it directly
async def fetch_bloomberg_data(api_url, jwt_token):
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Accept': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data

async def main():
    # Replace these with your actual URLs and credentials
    auth_url = 'https://your-bloomberg-auth-service.com/token'
    api_url = 'https://your-bloomberg-webservice.com/data'
    client_id = 'your_client_id'
    client_secret = 'your_client_secret'

    # Obtain JWT token (if necessary)
    jwt_token = await get_jwt_token(auth_url, client_id, client_secret)

    # Fetch data using the JWT token
    data = await fetch_bloomberg_data(api_url, jwt_token)
    print(data)

# Run the async main function
if __name__ == '__main__':
    asyncio.run(main())
