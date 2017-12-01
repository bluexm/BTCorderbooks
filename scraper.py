# coding=utf-8
import os 
import ccxt
import pandas as pd 
import time, datetime
import scraperwiki 

import sqlite3
from sqlite3 import Error

FREQ=30 # frequency of retrieveing data in seconds, 0 : run once (snapshot) 
LOCAL_SCRAPING = False #True if scraping is on the local machine, False if on morph.io

if LOCAL_SCRAPING:
	DB_FILE = '.\\orderbooks.db'
	""" create a database connection to a SQLite database """
	try:
		CONNEXION = sqlite3.connect(DB_FILE)
		print(sqlite3.version)
	except Error as e:
		print(e)

#bitmex = ccxt.bitmex()

## ANX PRO SETTINGS 
anx = ccxt.anxpro()
##Private key 
api_key = os.environ.get('ANX_API_KEY') 
api_secret = os.environ.get('ANX_SECRET')
api_key,api_secret = '',''
exmo = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Poloniex settings
polo = ccxt.poloniex()
##Private key 
api_key = os.environ.get('POLONIEX_API_KEY') 
api_secret = os.environ.get('POLONIEX_SECRET') 
api_key,api_secret = '',''
exmo_polo   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##BitMex settings
bmex = ccxt.bitmex()
##Private key 
api_key = os.environ.get('BITMEX_API_KEY') 
api_secret = os.environ.get('BITMEX_SECRET') 
api_key,api_secret = '','' 
exmo_bmex   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Bitfinex  settings
bfix = ccxt.bitfinex()
##Private key 
api_key = os.environ.get('BITFINEX_API_KEY') 
api_secret = os.environ.get('BITFINEX_SECRET') 
api_key,api_secret = '',''
exmo_bfix   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Gatecoin  settings
bgtcn = ccxt.gatecoin()
##Private key 


api_key,api_secret = '',''
exmo_bgtcn   = ccxt.exmo({
    'apiKey': '',
    'secret': ''
})


def get_orderbook(ccxtobj,symbol,sourcename):
	global CONNEXION
	dictbook = ccxtobj.fetch_order_book(symbol)
	print("{} storing orderbook ".format(datetime.datetime.now(),"%d/%m/%y %H:%M:%S")+ sourcename)
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])	
	b['rank']=b.index.values
	df = pd.merge(a,b,on='rank')
	df['source']=sourcename
	
	df.index = [str(dictbook['timestamp']) for  r in df['rank']] 
	
	# record in DB
	if LOCAL_SCRAPING:
		df.to_sql('orderbooks',connexion,if_exists='append')
	else: 
		# for morph.io
		df['timestamp']=df.index
		df['ukey']=[str(dictbook['timestamp'])+"_"+sourcename+"_"+str(r) for  r in df['rank']]
		dg = df.round(10)
		for k in range(len(dg)):
			#print(df.iloc[k].to_dict())
			di = dg.iloc[k].to_dict()
			#print(di)
			scraperwiki.sqlite.save(unique_keys=['ukey'],table_name="obooks", 
				data={
					'ukey':di['ukey'], 
					'timestamp':di['timestamp'],
					'timestamp': format(datetime.datetime.fromtimestamp(float(di['timestamp'])/1000),"%d/%m/%Y %H:%M:%S"),
					'source':di['source'],
					'rank':round(di['rank'],10),
					'bidqty':round(di['bidqty'],10), 
					'bidprice':round(di['bidprice'],10), 
					'askqty':round(di['askqty'],10), 
					'askprice':round(di['askprice'],10), 
					
					})
			#print("done")

i=0
while i<24*3600/FREQ:
	get_orderbook(bgtcn, "BTC/USD", 'gatecoin')	
	get_orderbook(anx, anx.symbols[0], 'anx')
	get_orderbook(polo, "BTC/USDT", 'polo')
	get_orderbook(bmex, "BTC/USD", 'bitmex')
	get_orderbook(bfix, "BTC/USD", 'bitfinex')
	
	time.sleep(FREQ)
	i+=1
	
if LOCAL_SCRAPING: 
	conn.close()

