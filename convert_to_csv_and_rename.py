#import packages, need Func file to run
#script can process detailed transaction reports for 167 and 405, as well as GP tag data files for SR 167
import pandas as pd, numpy as np, datetime, os, re, matplotlib.pyplot as plt
from Func import *

#Program Variables
start_script_time = datetime.datetime.now() #to calculate script runtime

#get list of files
list_of_files = get_files()

for file in list_of_files:
	if 'tcx' in file:
		print file[0:4] + file[5:7] + file[8:10] + '_T_' + 
	else:
		continue

#Script runtime
print '\n'
runtime = datetime.datetime.now() - start_script_time 
print 'program runtime = ' + str(runtime)
