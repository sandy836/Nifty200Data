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
from datetime import datetime
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
