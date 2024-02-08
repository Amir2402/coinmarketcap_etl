import requests as rq 
import json 
import os 
from dotenv import load_dotenv  

def get_json_data() : 
    load_dotenv() 
    my_key = os.getenv('X-CMC_PRO_API_KEY')
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    parameters =  {
        'start':'1' ,
        'limit':'50' ,
        'convert':'USD' , 
        'sort_dir' : 'desc'
    }

    headers = {
        'Accepts': 'application/json' ,
        'X-CMC_PRO_API_KEY' : my_key
    }

    session = rq.Session()
    session.headers.update(headers)

    try :
        response = session.get(url , params=parameters) 
        data = json.loads(response.text)
        return data 
    
    except : 
        return {} 
