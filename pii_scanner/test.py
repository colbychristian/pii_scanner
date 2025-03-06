import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
payload = {
    "model": "mistral",
    "prompt": "Hello, world!",
    "stream": False
}

try:
    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    print(response.json())
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")