import datetime
import pytz, urllib2
from bs4 import BeautifulSoup
import os, pdb, time, sys
import multiprocessing as mp
from multiprocessing.dummy import Pool
import pickle

import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web
from pandas import Series, DataFrame

'''
Necessary Libraries:
--------------------

bs4, pytz, urllib2, multiprocessing, progressbar2, pandas, pandas_datareader

-----------------------
Description:

Get ticker data for stocks in data/master_stock_data/master_stock_list.pkl
from start date to most current 

Can also add additional stock in add_custom_list

Also obtain ticker data for DJIA, RUSSELL2K, and SP500

'''

# string which holds directory of this file
file_prefix = os.path.dirname(os.path.realpath(__file__))

def configure_proxy(proxy_address=''):
	''' In case of proxy... '''
	import os
	os.environ['HTTPS_PROXY'] = proxy_address 
	os.environ['HTTP_PROXY'] = proxy_address

def split(seq, procs):
		'''
		split sequence into #procs
		seq must be a list

		for use if using >1 processor
		'''
		avg = len(seq) / float(procs)
		out = []
		last = 0.0
		while last < len(seq):
			out.append(seq[int(last):int(last + avg)])
			last += avg
		return out

# ---------------------------------------------------
# convert panels to data frame
# ---------------------------------------------------
# get "Close data"
def panels_to_df(r_panels):
	'''convert list of panels to a dictionary of dataframes'''
	data = {}
	price_types = list(r_panels[0].keys())
	for p in price_types:
		R = []
		for j in range(len(r_panels)):
			r_panel = r_panels[j]
			stocks = r_panel.minor_axis
			r = []
			for s in stocks:
				r_temp = r_panel[:,:,s]
				s_temp = r_temp.ix[:,p]
				s_temp.name=s
				r.append(s_temp)
			R += r
		data[p] = pd.concat(R,axis=1)
	return data, price_types

def get_index_data(start,end=None,behind_proxy=0):
	'''
	returns pandas Series for djia, nasdaq, etc.
	i..e, only index data
	'''
	if behind_proxy == 1:
		configure_proxy()

	print "Getting index data from FRED:"
	expire_after1 = datetime.timedelta(minutes=90)
	# session1 = requests_cache.CachedSession(cache_name='index_data_cache', backend='sqlite', expire_after=expire_after1)

	print "\tGetting sp500 index data..."
	sp500 = web.DataReader("SP500", 'fred', start=start)
	print "\tGetting djia index data..."
	djia = web.DataReader("DJIA", 'fred', start=start)
	print "\tGetting nasdaq index data..."
	nasdaq = web.DataReader("NASDAQCOM", 'fred', start=start)
	print "\tGetting russell index data..."
	russell2k = web.DataReader("RU2000PR", 'fred', start=start)

	print "\tWriting index data to csv..."
	# write sp500 data
	sp500.to_pickle(file_prefix + '/data/index_historical_sp500.pkl')
	# write djia data
	djia.to_pickle(file_prefix + '/data/index_historical_djia.pkl')
	# write nasdaq index history
	nasdaq.to_pickle(file_prefix + '/data/index_historical_nasdaq.pkl')
	# write russell index history
	russell2k.to_pickle(file_prefix + '/data/index_historical_russell.pkl')

	print "Done retrieving index data from FRED."
	return djia, nasdaq, sp500, russell2k

def get_historical_data(start,end=None,add_custom_list=[],np=4,behind_proxy=0,get_symbol_list=0,custom_range=None):
	'''
	returns dictionary of pandas dataframes
	the keys are 'Close', 'Open', etc...
	e.g. data['Close'] is a dataframe where each column is a stock, 
		and the rows are the dates

	each stock in the master stock list
		start: datetime start date
		end: default is None which gives data up to today
		add_custom_list: list of additional stocks to get data
		np: number of processors to split yahoo finance API calls
		behind_proxy: indicator is behind proxy (1 - yes, 0 - no)

	If there are any stocks not in the master list, that you want to add, add it to custom add_custom_list

	'''
	if behind_proxy == 1:
		configure_proxy()
	# ---------------------------------------------------
	# Get symbol list
	# ---------------------------------------------------
	if get_symbol_list == 1:
		from getStockSymbols import getStockSymbols
		getStockSymbols(n_split=int(round(np/2)),n_procs=np,behind_proxy=behind_proxy)
		# os.system('python getStockSymbols')

	# ---------------------------------------------------
	# load master symbol list
	# ---------------------------------------------------
	import pickle
	with open(file_prefix + "/data/master_stock_data/master_stock_list.pkl", 'rb') as f:
	    full_stock_list = pickle.load(f)


	# combine into master list
	if custom_range == None:
		master_stock_list = list(set(full_stock_list + add_custom_list))
	else:
		master_stock_list = list(set(full_stock_list + add_custom_list))[custom_range[0]:custom_range[1]]

	# ---------------------------------------------------
	# setup parallelization
	# ---------------------------------------------------
	# define function for parallel processing

	sp = split(master_stock_list,procs=np)

	# ---------------------------------------------------
	# define function in batch mode
	# ---------------------------------------------------
	def get_historical_data_batch(stock_list):
		print "    Getting data for %i stocks..." %(len(stock_list))
		source = "yahoo"
		r_panel = web.DataReader(stock_list, source, start=start)
		return r_panel

	# ---------------------------------------------------
	# run 
	# ---------------------------------------------------
	# concatenate all the dfs
	t0 = time.time()
	pool = Pool(processes=np)
	print "Getting history for %i stocks from sp500, nasdaq, and nyse.." %(len(master_stock_list))
	r_panels = pool.map(get_historical_data_batch,sp)
	data, price_types = panels_to_df(r_panels)
	print "It took %.2f seconds to get historical data on %i stocks." %(time.time() - t0, len(master_stock_list))

	print "Saving data..."
	for p in price_types:
		print "\t savings %s prices" %(p)
		ptype = p.replace(" ","_")
		filename = file_prefix + '/data/historical_' + ptype
		# save as pickled object
		data[p].to_pickle(filename + '.pkl')

	return data # dictionary of historical close, open, etc. data

if __name__ == '__main__':
	# ---------------------------------------------------
	# Define start time and indicators
	# ---------------------------------------------------
	start = datetime.datetime(2002, 1, 1)

	# set proxy (0 - no proxy, 1 - yes and edit configure_proxy)
	proxy_indicator = 0

	# ---------------------------------------------------
	# get historical ticker data
	# ---------------------------------------------------
	djia, nasdaq, sp500, russell2k = get_index_data(start,behind_proxy=proxy_indicator)

	# ---------------------------------------------------
	# get historical ticker data
	# ---------------------------------------------------
	data = get_historical_data(start,add_custom_list=[],np=8,behind_proxy=proxy_indicator)








