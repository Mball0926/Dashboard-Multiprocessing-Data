import numpy as np
import pandas as pdsleep
import random

import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
import json
import re

#Globally declare dataframe
from pandas import Series, DataFrame
import pandas as pd
import datetime as dt
import multiprocessing
from multiprocessing import Pool
from contextlib import closing
from random import randint
from time import sleep

y=1
while y!=2:
	r=requests.get('http://ifcb-data.whoi.edu/mvco/api/feed/temperature/start/2016-06-01/end/2016-07-02')
	#check for status return code 200 to see if request.get is successful. 
	if r.status_code==200:
		y=2
	else:
		y=1
	
feed=r.json()
fd=pd.DataFrame(feed)
Py=pd.to_datetime(fd.date)
fd=fd.set_index(Py,drop=False)
fd = fd.drop('date', 1)
fd=fd.drop('temperature',1)
fd['pid']=fd['pid']+'_class_scores.csv'
NUM=len(fd.index)




#function grabs PID from dataframe
def get_pid(fd,arg):
	
	k=arg
	url=fd['pid'][k]
	return url
	
	
def do_counts(args):
	arg, filename = args
	
	k=arg
	#for k in range(0,NUM):
	URL=get_pid(fd,arg)
		
	try:
		dataf=pd.read_csv(URL,index_col='pid')
	except:
		return None
	
	counts = dataf.idxmax(axis=1).value_counts().to_frame().T.sort_index(axis=1)
	counts.to_csv(filename, index=False)

		
		#request okay
	

#from multiprocessing import Pool
if __name__ == '__main__':
	
	
	
	
	pool = Pool()
	arg = range(NUM)
	filename = ['counts%1d.csv'% a for a in arg]
	pool.map(do_counts, zip(arg,filename))
	
	sleep(random.uniform(0.0,0.10))

	
	
	fout=open("out.csv","a")
	#first file:
	for line in open("counts0.csv"):
			fout.write(line)
	#now the rest:    
	for num in range(1,NUM):
		if os.path.exists("counts"+str(num)+".csv"):
			f = open("counts"+str(num)+".csv")
			f.next() # skip the header
			for line in f:
				fout.write(line)
				#f.close() # not really needed
				
			
		else:
			None
	fout.close()

