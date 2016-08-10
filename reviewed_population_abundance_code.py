#THIS CODE IS USED TO GRAB DATA FROM THE IFCB DASHBOARD AS WELL AS GRAB A LOCAL MAT FILE THAT CONTAINS DATA ALL NEEDED
#TO CALCULATE POPULATION ABUNDANCE FOR TIMES RANGEGING FROM 2006 UP TILL NOW

#created by Matthew Ball on August 10, 2016
#required file: ml_analyzed_all.mat
#output files: Population_Abundance.csv


import numpy as np
import pandas as pd
import random
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
import json
import re
from pandas import Series, DataFrame
from pandas import Categorical
import datetime as dt
import multiprocessing
from multiprocessing import Pool
from contextlib import closing
from random import randint
from time import sleep
import os
from scipy.io import loadmat
#THIS PORTION OF THE CODE DECLARES GLOBAL DATA AND VARIABLES AND DATAFRAMES
#Send out request and retry if request are unsuccessful 

while True:
	r=requests.get('http://ifcb-data.whoi.edu/mvco/api/feed/temperature/start/2016-06-10/end/2016-06-11')
	#check for status return code 200 to see if request.get is successful. 
	if r.status_code==200:
		break
	else:
		time.sleep(1)
	
feed=r.json()
#Integrate data into Global Dataframe and drop unnessessary data
fd=pd.DataFrame(feed)
Py=pd.to_datetime(fd.date)
fd=fd.drop('temperature',1)
#Grab pid so that when loading the mat file, you can align data based on pid and merge the data
fd['pid'] = fd['pid'].str.lstrip('http://ifcb-data.whoi.edu/mvco/')
#this line calls for a mat file that was stored locally from the network attached storage that holds data on water volumes for each bin
k=loadmat('ml_analyzed_all.mat',squeeze_me=True)
D=list(k['filelist_all'])
V=list(k['ml_analyzed'])
gd=pd.DataFrame(D)
gd.columns = ['pid']
gd["Volume"]=V
result = pd.merge(fd, gd, how='left')
#add csv file extension back on since merge is complete so that you can read the pid to get binned data
result['pid']='http://ifcb-data.whoi.edu/mvco/'+result['pid']+'_class_scores.csv'
#fd['pid']=fd['pid']+'_class_scores.csv'
NUM=len(result)


#THIS PORTION OF THE CODE INOVLCES TWO FUNCTIONS CALLD: get_pid and do_counts WHICH WILL BE CALLED BY MAIN 

#function grabs PID from dataframe
def get_pid(result,arg):
	
	
	url=result['pid'][arg]
	return url
	
#function produces abundance counts of phytoplankton and stores them in individually generated csv files
def do_counts(args):
	arg, filename = args

	
	#call function to grab URL
	URL=get_pid(result,arg)
		
	try:
	#read data from URL into dataframe
		dataf=pd.read_csv(URL,index_col='pid')
	except:
	#if reading URL fails do nothing because this means URL data does not exist
		return None
	#find maximum values for each column(e.g Phytoplankton classes) and divide by volume of water from IFCB inflow for 
	#population abundance
	
	abundance = dataf.idxmax(axis=1)
	Try=pd.Categorical(abundance,categories=dataf.columns)
	Abundance=Try.value_counts().to_frame().T.sort_index(axis=1)
	Abundance.index=[fd['date'][arg]]
	#if no water volume available return nothing and store in csv file
	if result['Volume'][arg] is None:
		Abundance=None
		Abundance.to_csv(filename, index=True)
	#if there is volume data, divide for population abundance and store in csv file
	else:
		Abundance=Abundance/result['Volume'][arg]
		Abundance.to_csv(filename, index=True)

		
		
	


if __name__ == '__main__':
	
	
	
	#from multiprocessing import Pool to utilize batch processing
	pool = Pool()
	arg = range(NUM)
	#call do_counts function and generate file with batch processing
	filename = ['Abundance%1d.csv'% a for a in arg]
	pool.map(do_counts, zip(arg,filename))
	 
	

	
	#Now open and create a file that will be used in the end to hold all data from all individually created files earlier
	fout=open("Population_Abundance.csv","a")
	#first file to read in column name and data once
	for line in open("Abundance0.csv"):
			fout.write(line)
	#now the rest:    
	for num in range(1,NUM):
		if os.path.exists("Abundance"+str(num)+".csv"):
			with open("Abundance"+str(num)+".csv") as f:
				f.next() # skip the header
				for line in f:
					fout.write(line)
				
				
			
		else:
			pass
	fout.close()
