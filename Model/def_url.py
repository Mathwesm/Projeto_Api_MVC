import requests
from dotenv import load_dotenv
import os
import time

load_dotenv()


url = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}


def chamar_api_myfinance(url):# -> list[json]:
    lista_dados_api = []
    cursor = 0

    while True:
        response = requests.get(url, headers=headers, params={"cursor": cursor})
        response_ajustado_json = response.json()
        dados_response = response_ajustado_json.get("response", None)
        
        if dados_response is not None:
            results = dados_response.get('results', [])
            remaining = dados_response.get('remaining', 0)
            lista_dados_api.extend(results)

            if remaining <= 0:
                break
        else:
            break
        
        cursor += 100
        time.sleep(1)

    return lista_dados_api