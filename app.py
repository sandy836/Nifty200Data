from flask import Flask, render_template, request, json
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore 
import os
import requests
import time
from requests.structures import CaseInsensitiveDict
import pandas as pd
import logging
import string
import json
import random
from datetime import datetime, time
import pymongo

LOG_FORMAT = '%(asctime)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
SIZE = 32
DATE = None
SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
FIREBASE_CONFIG_PATH = os.path.join(SITE_ROOT, 'static/auth', 'FireBaseConfig.json')
MONGO_CONFIG_PATH = os.path.join(SITE_ROOT, 'static/auth', 'MongoConfig.json')
SYMBOLS = os.path.join(SITE_ROOT, 'static/dataSchema', 'ind_nifty200list.csv')
PRICE_PATH = os.path.join(SITE_ROOT, "static/dataSchema", "price.json")
PRICEDETAILS_SCHEMA = os.path.join(SITE_ROOT, "static/dataSchema", "priceDetails.json")
MONGO_CONNECTION_DETAILS = json.load(open(MONGO_CONFIG_PATH))
MONGO_CONN_OBJ_REMOTE = pymongo.MongoClient(MONGO_CONNECTION_DETAILS["connectionStringRemote"])
MONGO_DB_OBJ_REMOTE = MONGO_CONN_OBJ_REMOTE.Nifty_200_Data
MONGO_CONN_OBJ_LOCAL = pymongo.MongoClient(MONGO_CONNECTION_DETAILS["connectionStringLocal"])
MONGO_DB_OBJ_LOCAL = MONGO_CONN_OBJ_LOCAL.Nifty_200_Data


#Initialize Flask app
firebaseConfig = json.load(open(FIREBASE_CONFIG_PATH))
LOG.info('Initialize App')
cred = credentials.Certificate(firebaseConfig)
firebase_admin.initialize_app(cred)
LOG.info('Initailized DB client')
dbObject = firestore.client()
app = Flask(__name__)

def apiRequest(symbol, requestId, candle_size):
    url = 'https://groww.in/v1/api/charting_service/v2/chart/exchange/NSE/segment/CASH/{}/daily?intervalInMinutes={}'.format(symbol, candle_size)
    headers = CaseInsensitiveDict()
    headers["authority"] = "groww.in"
    headers["pragma"] = "no-cache"
    headers["cache-control"] = "no-cache"
    headers["sec-ch-ua"] = '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"'
    headers["x-app-id"] = "growwWeb"
    headers["sec-ch-ua-mobile"] = "?0"
    headers["authorization"] = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ7XCJlbWFpbElkXCI6XCJzYW5kZWVwc2hyaXZhc3RhdmE1MThAZ21haWwuY29tXCIsXCJ1c2VyQWNjb3VudElkXCI6XCJBQ0M0MDY1OTI4ODg3NzM2XCIsXCJwbGF0Zm9ybVwiOlwid2ViXCIsXCJwbGF0Zm9ybVZlcnNpb25cIjpudWxsLFwib3NcIjpudWxsLFwib3NWZXJzaW9uXCI6bnVsbCxcImlwQWRkcmVzc1wiOlwiMjAwMTo0MjA6YzBlMDoxMDAxOjoxMixcIixcIm1hY0FkZHJlc3NcIjpudWxsLFwidXNlckFnZW50XCI6XCJNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMF8xNV83KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvOTEuMC40NDcyLjExNCBTYWZhcmkvNTM3LjM2XCIsXCJncm93d1VzZXJBZ2VudFwiOm51bGwsXCJkZXZpY2VJZFwiOm51bGwsXCJzZXNzaW9uSWRcIjpcImZiY2Y1OTFmLTllM2MtNDE4Zi05MDYzLTMyY2I0YzgwODZiM1wiLFwic3VwZXJBY2NvdW50SWRcIjpcIkFDQzQwNjU5Mjg4ODc3MzZcIn0iLCJuYmYiOjE2MjUxMTkzMTUsImlzcyI6Imdyb3d3YmlsbGlvbm1pbGxlbm5pYWwiLCJleHAiOjE2Mjc3MTEzNjUsImlhdCI6MTYyNTExOTM2NX0.uqRijINNiW4QbtGUgx0lRkSlv8U0hgYgdMsL0qhYQ8p4hTmgPjZeOQ9iZqXDvaAqEOafLuWD0y0vFcvY61C1EA"
    headers["accept"] = "application/json, text/plain, */*"
    headers["x-request-checksum"] = "eXUxN3RvIyMjR2M4NVpHYzVJYWg2U1ZpRzVqbEtWbThLSi8rUzVaRzk5bGRDVlk0UXM4NWRNZ3ZPNEp3eVlVQ05ZUDZyRndUNG1OSDE0bUJ1bE9HdVQvS1d6bCsvdmtlSmI1NnNzNVVvTkwzUWhSczRYZG89"
    headers["user-agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    headers["x-platform"] = "web"
    headers["x-user-campaign"] = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ7XCJjcmVhdGVkQXRcIjpcIjIwMjEtMDctMDJUMDY6MDg6MTcuODEwKzAwMDBcIixcInVzZXJBY2NvdW50SWRcIjpcIkFDQzQwNjU5Mjg4ODc3MzZcIixcImlzU3VjY2Vzc1wiOnRydWUsXCJzZXNzaW9uSWRcIjpcImIwYjRjODkyLTI3MmEtNGUxMC05OWI2LTU4ZDNlOGQzN2MzMlwiLFwiaXBBZGRyZXNzXCI6XCIxMDMuMjQyLjE5Ny4xNTgsXCIsXCJkZXZpY2VJZFwiOm51bGwsXCJzdXBlckFjY291bnRJZFwiOlwiQUNDNDA2NTkyODg4NzczNlwifSIsIm5iZiI6MTYyNTIwNjA0NywiaXNzIjoiZ3Jvd3diaWxsaW9ubWlsbGVubmlhbCIsImV4cCI6MTYyNzc5ODA5NywiaWF0IjoxNjI1MjA2MDk3fQ.wm-TldMrMyCJk2CmrUmf_YIRW_DbITFk3st070mv7TLR2jmKfFzaWultISeQNk7ny0l8ciFI13F-ZZgxRGDSWw"
    headers["x-request-id"] = requestId
    headers["sec-fetch-site"] = "same-origin"
    headers["sec-fetch-mode"] = "cors"
    headers["sec-fetch-dest"] = "empty"
    headers["referer"] = "https://groww.in/stocks/indraprastha-gas-ltd"
    headers["accept-language"] = "en-GB,en-US;q=0.9,en;q=0.8"
    headers["Retry-After"] = "5"
    headers["cookie"] = 'bfdskfds=light; _gcl_au=1.1.1191727926.1624526435; _ga=GA1.2.124283876.1624526435; G_ENABLED_IDPS=google; G_AUTHUSER_H=0; lhtndhgfd=U2FsdGVkX19TAdVZvcth4kE4ThNh4VyaGeIWKwuIXYFjy76K4s3AsE/XjWavRv9uUtaf6+8CXxz796TzhN7Y6fPch2W7yOl+2//xw7wnOWCm2IyH79wE7vcYj2jb4S87HCNH910Y9J9tjCZw5FlG5A7VgujOPyTczpA2zOZp7sR67excWU+TrdXD9eNw4/pLs9VfmPYPr6J/PT8YUZf/d+mFRQk+k0lKp1JBjA1SGT3GIZzxkiBtKqfYVl/eZK+AhaR9ABfFVS6WNwdqpCHZPDyWs40UMuckwrswBy2/Gj7QFvZWo9VQ2Dfmk84tY/XuXA1kDHrbTCsIHUK8tX6XOdCJ7U3k8IqPtOWTwA+uy7aXNqa6plSMJqP7hIv/O/MqKSrzDc9OfsqOtQWgmsj4qmAzbk4BGLi0tv1WZna5xda/PPyG50A/+UQva9zDDdhsQqZkOo+OLhZhU1VB+nRLUPulbOJ4xy5k5fRpwFjqjWC6x4WsL6Fz+Etyop/YlEkcymZ0v1Jk6lunQbKTUQ3Hzxi5BaCvPhisqCrDNP+cPIc7sQWTgl1k0eBxgsRL6D21VjLb3Rtt1eixBv/YQAVCRA91z1ETj1zOjStpDlBeyXAZ0E1eDhJZj90Z7nUNGO5iwaxjzVcwTYjhpcrMTAdG1/P+Tlcm6Q+NNNHBJD/SBgxYPiWbCErT1nbT9wI6yEbT0lqoEfsko4KS75G8HlsrjOSYRfNzF70/wKHjMeO9RwDTryavy/LqIPjXhKCDyLQXo6q4PMG8Z+0E3zkflA+HwZPPbCv5FRXGebKYBfDn2V1+oWw5kqoxHIbbFxsVEf3lHHGXTzewLziFGVGunW7uO75RyMBKugXqAnPYyViiPEiHxAqs8aqetHe2P91F3W6JFJoTrkG56eFC0JAwWdXnhwUnV5HBux29aLOL+PnXA/ggNEsWcXIvfl4dF2ODv7stCpLgsLSN7RceU75gP5MAmlcgPF3YkMP+GEmV2dddwt+9OrJ2kpSYEOEyXvTws66GnV8zgfKy5s2e2s/CiKCbUApH1ZDbZ4SEcU35qgLk9rNoO8YQX11+qvtWFlL7ZI3Ol6PZ130LHX50t9aBZ/50uCQKP1xe5ymVVfjYbeddOJS7+CXbfVfNZszwf4TLBg6B2jdeqMpW0N4791iUGx8TmnAAFcUy8I3QKzzFi3GSTsjCeVRMTQ4WsL7dlwnQfBHYlokm2gGna9iEgaj1LvgjpdUpw4kdDEBDS1W021rk4qoJUxwjByB5W9P5qDQ3HkNb; g_state={"i_p":1624644062185,"i_l":1}; _gid=GA1.2.1035530354.1624743829; __cfruid=45f2f108c704a41a3a8d80ac8c074f78a865b9c2-1624890918; _gat_UA-76725130-1=1; arkeyt473rfh7834cd=U2FsdGVkX1/LCr26nP+lO9NHIDkbFhmt4gnLYeEuVIGOyHsxzzOXdhfZ4GAebFb5mZQeeArFOtHvZC/994SCQu6tiMJUkYSTcsd1qd/tiv1uUCAz4tJDI+FancyqF0oxGkxmdn64/mCTFU1aKVnxoQcxANrPNuJ5sKtdNTER2MCsykNlDlvYZdz/MY1YSOGHJ3X0eV+/t9B7Z+b9GUQZiHwCnK8LW4/ZUxa9uv1XGkXRNx4zbOwtaE4B7OIdSFMyFh3+wTK18Vu2a3D1oyuLSQaFrla5E01LSQ1/iLK6iwqpyHVJmyOnKom/4uhNmFZgBc0buiFaw6gvq2dphzJiI03h+ZinDBqyvHK+mLjnTMUSjYAhQaeQxm15qgxg0W7tKuH8ItrCXcWaHOJNnXxAYqf0ziKxT/SBEAV2GzoWY4w5yrtKViy7uIXSZezUhMAQ3tvq3+uOVZdLUEFAqvfJh5H9ss6GelesVscP3iNRPne8vkYXY41SUvMvNaW0yGNqZ80sH8QojWx0p3pBhMUkCrytnw7+D93p81vQBIOkBt//9xQxKhUG+Eqr1rC9dmG4tvyz3tViEWHZqXuY1hHuwyBRcB5l1mkhkiW8ZVOKdP9TMOGJY6KW9xAQgqUZBZW7Sn07ZKwmxFinC9pUP0+j2mgD6RiX8AnqgDZ/yb9ltYJve001FXHT3hx1zrJnnCwLMLF1xIM5VgGtoWUm8A1df/F7FUIYnU+gaqpaBZOPu0he/EdmBb6V/WIDapdS31hymwuoTI/oKBZEZbvS6eACepuNRjO+h92KFkl9/Ghu6+sSA3QNCiOW8kJIqa1MJsVP; AUTH_SESSION_ID=U2FsdGVkX19amRr2Qcz/FDte9SxVmG2k/9IZG6aUXUWo5On6Qew+5fz+o54YTdp1UBz/gBm5tFZcKL9L7UidxA=='
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return json.loads(resp.text)
    LOG.info("Failed to produce API response with status Code {}".format(resp.status_code))
    return  None

def getRequestId():
    chars=string.ascii_lowercase + string.digits
    requestId = ''.join(random.choice(chars) for _ in range(SIZE))
    return requestId[0:8]+'-'+requestId[8:12]+'-'+requestId[12:16]+'-'+requestId[16:20]+'-'+requestId[20:]

def covertToSchema(data, stockName, stockSymbol, candle_size):
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
    priceDetailsSchema["candleType"] = candle_size
    priceDetailsSchema["stockName"] = stockName
    priceDetailsSchema["date"] = datetime.fromtimestamp(priceList[0]["timeStamp"]).strftime('%Y-%m-%d')
    priceDetailsSchema["stockSymbol"] = stockSymbol
    priceDetailsSchema["priceList"] = priceList
    return priceDetailsSchema

@app.route('/', methods = ['GET'])
def home():
    return "Nifty 200 data havesting Project"

@app.route('/FirebBaseInsert', methods = ['GET'])
def insertDataFireBase():
    candle_size_list = [1, 5, 10, 15]
    df_symbols = pd.read_csv(SYMBOLS)
    for index, row in df_symbols.iterrows():
        for candle_size in candle_size_list:
            stockSymbol = row["Symbol"]
            stockName = row["Company Name"]
            requestId = getRequestId()
            response = apiRequest(stockSymbol, requestId, candle_size)
            if response:
                dataList = covertToSchema(response["candles"], stockName, stockSymbol, candle_size)
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

@app.route('/MongoDbInsert', methods = ['GET'])
def insertMongoDb():
    evnName = request.args.get("env")
    candle_size_list = [1, 5, 10, 15]
    df_symbols = pd.read_csv(SYMBOLS)
    for index, row in df_symbols.iterrows():
        for candle_size in candle_size_list:
            stockSymbol = row["Symbol"]
            stockName = row["Company Name"]
            requestId = getRequestId()
            response = apiRequest(stockSymbol, requestId, candle_size)
            if response:
                dataList = covertToSchema(response["candles"], stockName, stockSymbol, candle_size)
                DATE = dataList["date"]
                LOG.info("Push Data")
                if evnName == "REMOTE":
                    res = MONGO_DB_OBJ_REMOTE[stockSymbol].update(
                        {"$and":[{"candleType":candle_size},{"date": DATE}]},
                        dataList,
                        upsert=True
                    )
                    LOG.info("Information pushed to MongoDb {} env".format(evnName))
                    LOG.info(res)
                    LOG.info("Stock number {} having symbol as {} and candle size {} pushed properly".format(index, stockSymbol, candle_size))
                elif evnName == "LOCAL":
                    res = MONGO_DB_OBJ_LOCAL[stockSymbol].update(
                        {"$and":[{"candleType":candle_size},{"date": DATE}]},
                        dataList,
                        upsert=True
                    )
                    LOG.info("Information pushed to MongoDb {} env".format(evnName))
                    LOG.info(res)
                    LOG.info("Stock number {} having symbol as {} and candle size {} pushed properly".format(index, stockSymbol, candle_size))
                else:
                    LOG.info("Pass a proper env Name")
            time.sleep(2)
    return "Successfully pushed to MongoDB for the data "+DATE



if __name__ == '__main__':
    app.run(debug = True) 
