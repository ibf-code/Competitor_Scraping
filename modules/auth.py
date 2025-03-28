import requests

LOGIN_URL = "https://www.mayesh.com/api/auth/login"

def authenticate(email, password):
    session = requests.Session()
    payload = {"email": email, "password": password}
    headers = {"content-type": "application/json"}
    response = session.post(LOGIN_URL, json=payload, headers=headers)

    if response.status_code in [200, 201]:
        jwt_token = response.json()["data"]["token"]
        headers["Authorization"] =  f"Bearer {jwt_token}"
        print("🎉Logged in successfully with {jwt_token}")
        return session, headers
    else:
        print("💔Failed to log in. Wrong credentials?")
        return None, None
    