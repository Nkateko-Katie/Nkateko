import numpy as np
import pandas as pd
import math
import argparse
import csv
import gzip
import multiprocessing as mp
import time
import os
import glob
from multiprocessing import Pool
import time

parser = argparse.ArgumentParser()
parser.add_argument("Directory", help="The GDELT files directory")
parser.add_argument("latitude", help="the latitude of the location")
parser.add_argument("longitude", help="the longitude of the location")
parser.add_argument("distance", help = "within which distance")
parser.add_argument("n_procs", help = "Number of processes")
args = parser.parse_args()

path = args.Directory

'''
def readfile():
	p = mp.Pool()
	print(os.getpid)
	for files in glob.glob(path+"*.csv"):
		#file1 = files
		f = pd.read_csv(files, low_memory = False, sep = '\t', header =None)
		p = p.apply_async(process, [f]) 
		f =print('f is',f)
		p =print('print p is',p)
	p.close()
	p.join()
	
	return 


def readfile():
	t1 = time.time()
	for files in glob.glob(os.path.join(path,'*')):
		f = pd.read_csv(files, low_memory = False, sep = '\t', header =None)	
	return f
'''

def harvesine(directory):
	for files in glob.glob(os.path.join(path,'*')):
		f = pd.read_csv(files, low_memory = False, sep = '\t', header =None)	
	
	
	data = f.fillna(0.0)
	lat1 = float(args.latitude)
	long1 = float(args.longitude)
	lat2 = data[53]
	long2 = data[54]
	d = float(args.distance)
	n = int(args.n_procs)
	R = 6371 #km
	ID = data[0]
	Goldsteinscale = data[30]
	country = data[6]
	url = data[57]

	d_lat = abs(lat2 - lat1)
	d_long =  abs(long2 - long1)

	harve_sine = np.sin(d_lat/2)*np.sin(d_lat/2) + np.cos(lat1)*np.cos(lat2)*np.sin(d_long/2)*np.sin(d_long/2)
	g_c = 2*np.arctan2(np.sqrt(harve_sine),np.sqrt(1-harve_sine))
	dis = g_c * R

	events = []
	for i,j in enumerate(dis):
		#for j in enumerate(i):
		if j<d:
			events.append([ID[i], Goldsteinscale[i], url[i],])
	
	t2 = time.time()
	#print('process',n,'took', t2-t1,'s')	
	#return lat1,lat2,long1,long2


'''
#data = readfile().fillna(0.0)
lat1 = float(args.latitude)
long1 = float(args.longitude)
lat2 = data[53]
long2 = data[54]
d = float(args.distance)
#n = int(args.n_procs)
R = 6371 #km
ID = data[0]
Goldsteinscale = data[30]
country = data[6]
url = data[57]
print(harvesine(lat1,lat2,long1,long2))
'''

if __name__ == '__main__':
	
	lat1 = args.latitude
	long1 = args.longitude
	splits = [args.Directory[i::len(args.Directory)] for i in range(len(args.Directory))]
	p = Pool(int(args.n_procs))
	print(p.map(harvesine(args.Directory), splits))


#######

