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
import tempfile
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
import sys
#THIS PORTION OF THE CODE DECLARES GLOBAL DATA AND VARIABLES AND DATAFRAMES
#Send out request and retry if request are unsuccessful 

starttime=sys.argv[1]
endtime=sys.argv[2]

while True:
	r=requests.get('http://ifcb-data.whoi.edu/mvco/api/feed/temperature/start/'+starttime+"/end/"+endtime)
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
#the following lines grab a file that contains the volume of water taken for each sample
#the file is grabed from a google drive directory and is then stored in a temporary file
#once the data is put in this file. You can access the data through loadmat and then deleting the temporary file
f=requests.get("https://drive.google.com/uc?export=download&id=0BzoJnj-e6BWpQmRCWk1tdS1tUTQ")
fp = tempfile.TemporaryFile()
fp.write(f.content)
k=loadmat(fp,squeeze_me=True)
fp.close()
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
	
	
	tempdirect=tempfile.mkdtemp()
	#from multiprocessing import Pool to utilize batch processing
	pool = Pool()
	arg = range(NUM)
	#call do_counts function and generate file with batch processing in a temporary directory
	filename = [os.path.join(tempdirect,'Abundance%1d.csv'% a) for a in arg]
	pool.map(do_counts, zip(arg,filename))
	

	
	#Now a file will be opened in the local/current directory that will hold all the data at the end of execution
	fout=open("Population_Abundance.csv","a")
	#first file to read in column name and data once from the file in the temporary directory
	for line in open(filename[0]):
			fout.write(line)
	#now the rest:    
	for num in range(1,NUM):
		if os.path.exists(filename[num]):
			with open(filename[num]) as f:
				f.next() # skip the header
				for line in f:
					fout.write(line)
					
				
				
			
		else:
			pass
	fout.close()
#the following lines of code will check if the files exist in the temporary folder and then remove then
	for num in filename:
		if os.path.exists(num):
			os.remove(num)
			
		else:
			pass
#the last line of code with delete the temporary directory
os.rmdir(tempdirect)
