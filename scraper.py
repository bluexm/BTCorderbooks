# coding=utf-8
import os 
import ccxt
import pandas as pd 
import time, datetime
import scraperwiki 

import sqlite3
from sqlite3 import Error
DB_FILE = '.\\orderbooks.db'

""" create a database connection to a SQLite database """
try:
	conn = sqlite3.connect(DB_FILE)
	print(sqlite3.version)
except Error as e:
	print(e)

#bitmex = ccxt.bitmex()

## ANX PRO SETTINGS 
anx = ccxt.anxpro()
##Private key 
api_key = os.environ.get('ANX_API_KEY') 
api_secret = os.environ.get('ANX_SECRET')
exmo = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Poloniex settings
polo = ccxt.poloniex()
##Private key 
api_key = os.environ.get('POLONIEX_API_KEY') 
api_secret = os.environ.get('POLONIEX_SECRET') 
exmo_polo   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##BitMex settings
bmex = ccxt.bitmex()
##Private key 
api_key = os.environ.get('BITMEX_API_KEY') 
api_secret = os.environ.get('BITMEX_SECRET') 
exmo_bmex   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Bitfinex  settings
bfix = ccxt.bitfinex()
##Private key 
api_key = os.environ.get('BITFINEX_API_KEY') 
api_secret = os.environ.get('BITFINEX_SECRET') 
exmo_bfix   = ccxt.exmo({
    'apiKey': api_key,
    'secret': api_secret
})

##Gatecoin  settings
bgtcn = ccxt.gatecoin()
##Private key 
exmo_bgtcn   = ccxt.exmo({
    'apiKey': '',
    'secret': ''
})


def get_orderbook(ccxtobj,symbol,sourcename,connexion):
	dictbook = ccxtobj.fetch_order_book(symbol)
	print("{} storing orderbook ".format(datetime.datetime.now(),"%d/%m/%y %H:%M:%S"))
	a = pd.DataFrame(dictbook['asks'],columns=['askprice','askqty'])
	a['rank']=a.index.values
	b = pd.DataFrame(dictbook['bids'],columns=['bidprice','bidqty'])
	b['rank']=b.index.values
	df = pd.merge(a,b,on='rank')
	df['source']=sourcename
	
	df.index = [str(dictbook['timestamp']) for  r in df['rank']] 
	# record in DB
	#df.to_sql('orderbooks',connexion,if_exists='append')
	
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
				'bidqty':round(di['bidqty'],10), 
				'bidprice':round(di['bidprice'],10), 
				'askqty':round(di['askqty'],10), 
				'askprice':round(di['askprice'],10), 
				'source':di['source']
				})
		#print("done")

i=0
while (i<60):
	get_orderbook(bgtcn, "BTC/USD", 'gatecoin', conn)	
	#get_orderbook(anx, anx.symbols[0], 'anx', conn)
	#get_orderbook(polo, "BTC/USDT", 'polo', conn)
	#get_orderbook(bmex, "BTC/USD", 'bitmex', conn)
	#get_orderbook(bfix, "BTC/USD", 'bitfinex', conn)
	
	time.sleep(30)
	i+=1
	
conn.close()

