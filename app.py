from flask import Flask, render_template, request, json
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore 
import os
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import logging
import string
import json
import random
from datetime import datetime

LOG_FORMAT = '%(asctime)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
SIZE = 32
SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
CONFIG_URL = os.path.join(SITE_ROOT, 'static/auth', 'config.json')
SYMBOLS = os.path.join(SITE_ROOT, 'static/dataSchema', 'ind_nifty200list.csv')
PRICE_PATH = os.path.join(SITE_ROOT, "static/dataSchema", "price.json")
PRICEDETAILS_SCHEMA = os.path.join(SITE_ROOT, "static/dataSchema", "priceDetails.json")


#Initialize Flask app
firebaseConfig = json.load(open(CONFIG_URL))
LOG.info('Initialize App')
cred = credentials.Certificate(firebaseConfig)
firebase_admin.initialize_app(cred)
LOG.info('Initailized DB client')
dbObject = firestore.client()
app = Flask(__name__)

def apiRequest(symbol, requestId, candle_size):
    url = 'https://groww.in/v1/api/charting_service/v2/chart/exchange/NSE/segment/CASH/{}/daily?intervalInMinutes={}'.format(symbol, candle_size)
    headers = CaseInsensitiveDict()
    headers['authority'] = 'groww.in'
    headers['pragma'] = 'no-cache'
    headers['cache-control'] = 'no-cache'
    headers['sec-ch-ua'] = '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"'
    headers['x-app-id'] = 'growwWeb'
    headers['sec-ch-ua-mobile'] = '?0'
    headers['authorization'] = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ7XCJlbWFpbElkXCI6XCJzYW5kZWVwc2hyaXZhc3RhdmE1MThAZ21haWwuY29tXCIsXCJ1c2VyQWNjb3VudElkXCI6XCJBQ0M0MDY1OTI4ODg3NzM2XCIsXCJwbGF0Zm9ybVwiOlwid2ViXCIsXCJwbGF0Zm9ybVZlcnNpb25cIjpudWxsLFwib3NcIjpudWxsLFwib3NWZXJzaW9uXCI6bnVsbCxcImlwQWRkcmVzc1wiOlwiMjAwMTo0MjA6YzBlMDoxMDA2Ojo3MTMsXCIsXCJtYWNBZGRyZXNzXCI6bnVsbCxcInVzZXJBZ2VudFwiOlwiTW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTBfMTVfNykgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzkxLjAuNDQ3Mi4xMTQgU2FmYXJpLzUzNy4zNlwiLFwiZ3Jvd3dVc2VyQWdlbnRcIjpudWxsLFwiZGV2aWNlSWRcIjpudWxsLFwic2Vzc2lvbklkXCI6XCJlYWUwNmQ0NC0zN2MxLTQ4MDEtOTY0Zi1jMTY3ZWMyYjBlNjRcIixcInN1cGVyQWNjb3VudElkXCI6XCJBQ0M0MDY1OTI4ODg3NzM2XCJ9IiwibmJmIjoxNjI0NjM2ODA1LCJpc3MiOiJncm93d2JpbGxpb25taWxsZW5uaWFsIiwiZXhwIjoxNjI3MjI4ODU1LCJpYXQiOjE2MjQ2MzY4NTV9.MAqmvvfcTnwdHmdqFOMbm8sl9rqPuTTn_1BZ8dukNKByHK2C278lAhewZW7y46tyl4dGnruJzQS6HXdW9jL8rg'
    headers['accept'] = 'application/json, text/plain, */*'
    headers['x-request-checksum'] = 'cnd3bGRwIyMjSHYwZWZmMFZhZXNFdDVLTEorWEt0eUdvcnVkZkdYYmZFOERxcW9pSHRiY2FvWHByY1lJU3hHc0xOZXlOVEVIZEJtUEdiVjh5c3hQSDZPM01sT0R3VDJZci93eEVVYitNODRuejUwRDU3UFk9'
    headers['user-agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    headers['x-platform'] = 'web'
    headers['x-user-campaign'] = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ7XCJjcmVhdGVkQXRcIjpcIjIwMjEtMDYtMjhUMTQ6MzU6MjIuNjM2KzAwMDBcIixcInVzZXJBY2NvdW50SWRcIjpcIkFDQzQwNjU5Mjg4ODc3MzZcIixcImlzU3VjY2Vzc1wiOnRydWUsXCJzZXNzaW9uSWRcIjpcImUwMDgxNDE2LTQ4OWQtNDEyMy05ZjZjLTQyM2Q5Yzc0ZmZiYVwiLFwiaXBBZGRyZXNzXCI6XCIyMDAxOjQyMDpjMGUwOjEwMDY6Ojc5NixcIixcImRldmljZUlkXCI6bnVsbCxcInN1cGVyQWNjb3VudElkXCI6XCJBQ0M0MDY1OTI4ODg3NzM2XCJ9IiwibmJmIjoxNjI0ODkwODcyLCJpc3MiOiJncm93d2JpbGxpb25taWxsZW5uaWFsIiwiZXhwIjoxNjI3NDgyOTIyLCJpYXQiOjE2MjQ4OTA5MjJ9.ww1G6ioA6Vv0UDyyPRSq84L7rlzHo1HwReXD3UOh5Pk8OUcfq-W5qpW2bPY5FoS64bN5F9GgWT88lG-avElslA'
    headers['x-request-id'] = requestId
    headers['sec-fetch-site'] = 'same-origin'
    headers['sec-fetch-mode'] = 'cors'
    headers['sec-fetch-dest'] = 'empty'
    headers['accept-language'] = 'en-GB,en-US;q=0.9,en;q=0.8'
    headers['cookie'] = 'bfdskfds=light; _gcl_au=1.1.1191727926.1624526435; _ga=GA1.2.124283876.1624526435; G_ENABLED_IDPS=google; G_AUTHUSER_H=0; lhtndhgfd=U2FsdGVkX19TAdVZvcth4kE4ThNh4VyaGeIWKwuIXYFjy76K4s3AsE/XjWavRv9uUtaf6+8CXxz796TzhN7Y6fPch2W7yOl+2//xw7wnOWCm2IyH79wE7vcYj2jb4S87HCNH910Y9J9tjCZw5FlG5A7VgujOPyTczpA2zOZp7sR67excWU+TrdXD9eNw4/pLs9VfmPYPr6J/PT8YUZf/d+mFRQk+k0lKp1JBjA1SGT3GIZzxkiBtKqfYVl/eZK+AhaR9ABfFVS6WNwdqpCHZPDyWs40UMuckwrswBy2/Gj7QFvZWo9VQ2Dfmk84tY/XuXA1kDHrbTCsIHUK8tX6XOdCJ7U3k8IqPtOWTwA+uy7aXNqa6plSMJqP7hIv/O/MqKSrzDc9OfsqOtQWgmsj4qmAzbk4BGLi0tv1WZna5xda/PPyG50A/+UQva9zDDdhsQqZkOo+OLhZhU1VB+nRLUPulbOJ4xy5k5fRpwFjqjWC6x4WsL6Fz+Etyop/YlEkcymZ0v1Jk6lunQbKTUQ3Hzxi5BaCvPhisqCrDNP+cPIc7sQWTgl1k0eBxgsRL6D21VjLb3Rtt1eixBv/YQAVCRA91z1ETj1zOjStpDlBeyXAZ0E1eDhJZj90Z7nUNGO5iwaxjzVcwTYjhpcrMTAdG1/P+Tlcm6Q+NNNHBJD/SBgxYPiWbCErT1nbT9wI6yEbT0lqoEfsko4KS75G8HlsrjOSYRfNzF70/wKHjMeO9RwDTryavy/LqIPjXhKCDyLQXo6q4PMG8Z+0E3zkflA+HwZPPbCv5FRXGebKYBfDn2V1+oWw5kqoxHIbbFxsVEf3lHHGXTzewLziFGVGunW7uO75RyMBKugXqAnPYyViiPEiHxAqs8aqetHe2P91F3W6JFJoTrkG56eFC0JAwWdXnhwUnV5HBux29aLOL+PnXA/ggNEsWcXIvfl4dF2ODv7stCpLgsLSN7RceU75gP5MAmlcgPF3YkMP+GEmV2dddwt+9OrJ2kpSYEOEyXvTws66GnV8zgfKy5s2e2s/CiKCbUApH1ZDbZ4SEcU35qgLk9rNoO8YQX11+qvtWFlL7ZI3Ol6PZ130LHX50t9aBZ/50uCQKP1xe5ymVVfjYbeddOJS7+CXbfVfNZszwf4TLBg6B2jdeqMpW0N4791iUGx8TmnAAFcUy8I3QKzzFi3GSTsjCeVRMTQ4WsL7dlwnQfBHYlokm2gGna9iEgaj1LvgjpdUpw4kdDEBDS1W021rk4qoJUxwjByB5W9P5qDQ3HkNb; g_state={"i_p":1624644062185,"i_l":1}; _gid=GA1.2.1035530354.1624743829; __cfruid=45f2f108c704a41a3a8d80ac8c074f78a865b9c2-1624890918; arkeyt473rfh7834cd=U2FsdGVkX1/LwnyCWvIAxI+k3kwHDR93cKRkOaj0XO86et+FCVJDc+7HTEuUCB6wFLM43UrCoSEfOZkb7mxyPrPCdavwDkprPfyinPNjf8fnNXFagAGHNf6MXxRqX9Qdh78+7YioSL5Rcp9URSDN7OOsfhYk+e2lYzlqX86HG7G3bjC31qszRVOO4eRavcNmPjkVjmmrZ4jruWdjsxaPGcmhfgbIpwDPuDB7BJEMj7qq8SOA9vX9/gNSuJwC8IXk0z1D/0uhjis7XvFjF3Vo5V0xixgKB4In7PrczWNL3N/rcUKnQA9IiooTe3CRcIPdK0XQWBdb/++0g872A8J6FuwPQxgmhyF7OOMcQ9EZty1Cs07NZ+lkPk9dj0VTGz5Q0/1BzMtgAbDRPz4+u+glTp5Ah6KXm2r/wEFK3z1hVDNPJjyOyV61fnkTbRfxLvgpaVG6gXdShfzDyZTiQNZBpeOtLv8qpQeEd65xImWPE0JtJkU1aWhHzsEdM+MRofZrLsIMgMSsfLkvsjBoOR6rAPewE4t6gcoMbDf4+9YGNy+BmhzS+YCBREmcPRNISwWCN2N2FSUlqYRb20nF+BgnPQzMAkT7z5W69FZ7JTxjEufJG03QHv+lY0gILFYM/5g5G4D03NdoslV8fvCEEqQAFMLgLnnHSwaRrTMIaMTCO31pQCV+EY732OVWydEi7i0oxz95tAN8x+202D7ff/pxf7EO7lYqiGe3TjuL27cVhYH+JhNW6hjDn+zmgplQ8HTs07zA6M1rHXCWVHNos/npMpQ/6+OJ4vw+9s5ZS21JLtO4JNLtXyLq1vvHi6Z1nSgO; AUTH_SESSION_ID=U2FsdGVkX1/UOMV1Y+3W6svhnO8Nz2V3eSIp8K6sEzQJamd45lL+RNxmDFKzyEiR990X4OfytKQRbaMFBQHGiw==; _gat_UA-76725130-1=1'

    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return json.loads(resp.text)
    return  None

def getRequestId():
    chars=string.ascii_lowercase + string.digits
    requestId = ''.join(random.choice(chars) for _ in range(SIZE))
    return requestId[0:8]+'-'+requestId[8:12]+'-'+requestId[12:16]+'-'+requestId[16:20]+'-'+requestId[20:]

def covertToSchema(data, stockName, stockSymbol):
    priceList = []
    priceDetailsSchema = json.load(open(PRICEDETAILS_SCHEMA))
    for price in data:
        if not ((price[1] is None) or (price[2] is None) or (price[3] is None) or (price[4] is None)):
            priceSchema = json.load(open(PRICE_PATH))
            priceSchema["timeStamp"] = price[0]
            priceSchema["open"] = price[1]
            priceSchema["high"] = price[2]
            priceSchema["low"] = price[3]
            priceSchema["close"] = price[4]
            #if the volumne traded is null
            priceSchema["volumn"] = 0 if not price[5] else price[5]
            priceList.append(priceSchema)
    priceDetailsSchema["stockName"] = stockName
    priceDetailsSchema["date"] = datetime.fromtimestamp(1624851420).strftime('%Y-%m-%d')
    priceDetailsSchema["stockSymbol"] = stockSymbol
    priceDetailsSchema["priceList"] = priceList
    return priceDetailsSchema

@app.route('/', methods = ['GET'])
def insertData():
    DATE = None
    candle_size_list = [5, 10, 15]
    df_symbols = pd.read_csv(SYMBOLS)
    for index, row in df_symbols.iterrows():
        for candle_size in candle_size_list:
            stockSymbol = row["Symbol"]
            stockName = row["Company Name"]
            requestId = getRequestId()
            response = apiRequest(stockSymbol, requestId, candle_size)
            if response:
                dataList = covertToSchema(response["candles"], stockName, stockSymbol)
                DATE = dataList["date"]
                uniqueId = DATE+"_"+stockSymbol
                LOG.info("Push Data")
                res = dbObject.collection('Nifty_200_Data').document(stockSymbol)\
                    .collection("CandleStick_"+str(candle_size)).document(uniqueId)\
                    .set(dataList)
                LOG.info("Information pushed to Db")
                LOG.info(res)
                LOG.info("Stock number {} having symbol as {} and candle size {} pushed properly".format(index, stockSymbol, candle_size))
    return "Successfully pushed to Firebase for the data "+DATE

if __name__ == '__main__':
    app.run(debug = True) 
