import requests
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

url_category = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/category/"
url_recipient = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/recipient/"


def chamar_api_myfinance(url):
    response = requests.get(url, headers=headers)
    return response

response_category = chamar_api_myfinance(url_category)
response_recipient = chamar_api_myfinance(url_recipient)


category_ajustado_json = response_category.json()['response']['results']
recipient_ajustado_json = response_recipient.json()['response']['results']


df_category = pd.DataFrame(category_ajustado_json,columns=['title', '_id'])
df_recipient = pd.DataFrame(recipient_ajustado_json,columns=['title', '_id','category_ref'])


df_category.to_excel('Category.xlsx', index=False)
df_recipient.to_excel('Recipient.xlsx', index=False)

