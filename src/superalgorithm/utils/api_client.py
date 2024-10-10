import requests
import hmac
import hashlib
from superalgorithm.utils.config import config
from superalgorithm.utils.helpers import get_now_ts, guid


def _sign_request(secret, nonce, timestamp):
    message = f"{nonce}:{timestamp}"
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def api_call(
    method,
    data=None,
    json=None,
    files=None,
):
    base_url = config.get("SUPER_API_ENDPOINT")
    api_key = config.get("SUPER_API_KEY")
    secret = config.get("SUPER_API_SECRET")

    if not base_url or not api_key or not secret:
        raise Exception("API endpoint, key, or secret is missing  in the config")

    url = f"{base_url}/{method}"

    nonce = guid()
    timestamp = str(get_now_ts())
    signature = _sign_request(secret, nonce, timestamp)

    headers = {
        "X-API-Key": api_key,
        "X-Nonce": nonce,
        "X-Timestamp": timestamp,
        "X-Signature": signature,
    }

    # when sending json data to the api we have to use the json attribute of the request.post method,
    # when uploading files, we sent data and files
    response = requests.post(url, headers=headers, json=json, data=data, files=files)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def upload_log(data_dict):
    return api_call("v1-monitor", json=data_dict)
