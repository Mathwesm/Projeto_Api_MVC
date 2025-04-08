import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
import pandas as pd
from Model.def_url import chamar_api_myfinance
load_dotenv()

url = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

lista_dados_api = chamar_api_myfinance(url)

df = pd.DataFrame(lista_dados_api)

df.to_excel('FTransactions.xlsx', index=False)