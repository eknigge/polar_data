import pandas as pd
import numpy as np
import datetime
import os, re, matplotlib.pyplot as plt
from scipy.signal import savgol_filter
from scipy.signal import medfilt



#Program Variables
start_script_time = datetime.datetime.now() #to calculate script runtime

#Function to get files in cwd
def get_files():
	current_dir = os.getcwd()
	files = os.listdir(current_dir)
	return files

#function to open GP Tag Data File
def open_GP_tag_file(file):
	df = pd.read_excel(file)
	df['Start_Time'] = pd.to_datetime(df['READERTMST'])
	df['READERTMST'] = pd.to_timedelta(pd.to_datetime(df['READERTMST']))
	df = df.drop('LANEINDEX',1)
	df['HOSTNAME'] = df['HOSTNAME'].str.slice(3,7)
	df['Plaza'] = df['HOSTNAME']
	df.to_pickle('pickle_GP_tag_data')
	df.rename_axis({"HOSTNAME":"Plaza"},axis="columns")
	return df

def open_167_csv_tag_file(file):
	df = pd.read_csv(file,sep=',',header=None,names=['Plaza','Date','Fill2','Agency','TAGID'])
	df['Start_Time'] = pd.to_datetime(df['Date'])
	df['READERTMST'] = pd.to_timedelta(pd.to_datetime(df['Date']))
	df['Plaza'] = df['Plaza'].str.slice(3,7)
	df['Date'] = df['Date'].str.rstrip()
	df['TAGID'] = df['TAGID'].str.rstrip()
	df = df.dropna()#drop all rows that have NaN values
	df = df.drop('Fill2',1)
	df.to_pickle('pickle_GP_tag_data')
	return df


#Function to open 405 excel file
def open_xls_405(filename): 
    df = pd.read_excel(filename, parse_cols=[0, 1, 3, 4, 17, 18, 22], header=None, parse_dates=True, infer_datetime_format=True) #open file
    df = df[6:] #remove header information
    df = df.dropna(subset=[5])#drop data wo/tags
    df.reset_index(drop=True, inplace=True) #reset index values after dropping data
    df.columns = ['trx_id','lane', 'trx_time','trx_type','agency','tag_num','tag_read_time'] #rename column names
    pd.df['trx_time']
    return df #return dataframe


def open_405_detailed_trips(file):
	sheet_num = 0
	try:
		df = pd.read_excel(file,sheet_name=1, header = 5)
		sheet_num = 1
	except IndexError:
		df = pd.read_excel(file,sheet_name=0, header = 5)
		sheet_num = 0
	try:
		df['Trx DateTime'] = pd.to_datetime(df['Trx DateTime'])
	except KeyError:
		df = pd.read_excel(file,sheet_name=sheet_num, header = 6)
		df['Trx DateTime'] = pd.to_datetime(df['Trx DateTime'])
	df['Lane'] = df['Lane'].str.slice(0,4)
	df = df.dropna(axis=0,subset=['Number'])
	df['READERTMST'] = pd.to_timedelta(df['Trx DateTime'])
	df = df.rename({"Lane":"Plaza","Number":"TAGID","Trx DateTime":"Start_Time"},axis="columns")
	df.to_pickle(('pickle_detailed_trx_' + file).rstrip('.xlsx'))
	return df

def open_167_detailed_trips(file):
	df = pd.read_excel(file, header = 5)
	df['Trx DateTime'] = pd.to_datetime(df['Trx DateTime'])
	df['Lane'] = df['Lane'].str.slice(3,7)
	df = df.dropna(axis=0,subset=['Number'])
	df['READERTMST'] = pd.to_timedelta(df['Trx DateTime'])
	df = df.rename({"Lane":"Plaza","Number":"TAGID","Trx DateTime":"Start_Time"},axis="columns")
	df.to_pickle(('pickle_detailed_trx_' + file).rstrip('.xlsx'))
	return df

#Function to get tags from I-405 dataframe
def gettags(dataframe):
	tag_list = dataframe['tag_num']
	tag_list = tag_list.unique() #remove duplicate tags
	return tag_list #return list

#Function to find tag in dataframe, returns indices
def tag_index(dataframe,tag):
	df = dataframe.isin([tag])
	df_tag_index = df[df['tag_num'] == True] #Reduce dataframe to rows where tag found
	return  df_tag_index.index #list of rows

#Function to extract matching tag rows, returns filtered_dataframe
def match_tag_extract(tag_index,dataframe):
#	if len(tag_index) == 1:
#		return
	count = 0
	for element in tag_index:
		if count == 0: #create dataframe
			df = dataframe.iloc[element:element+1:]
			count += 1
		else:  #append matching rows
			df_append = dataframe.iloc[element:element+1:]
			frames = [df, df_append]
			df = pd.concat(frames)
			df = df.sort_values(by='tag_read_time', ascending=False) #sort values
			df['lane'] = df['lane'].str.slice(start=0,stop=4)

	return df 

#Function to calculate trips from filtered_dataframe
def create_trips(filtered_dataframe): 

	tag_value = filtered_dataframe.loc[filtered_dataframe.index[0],'tag_num']
	end_time = filtered_dataframe.loc[filtered_dataframe.index[0],'tag_read_time']
	start_time = filtered_dataframe.loc[filtered_dataframe.index[filtered_dataframe.shape[0] - 1],'tag_read_time']
	trx_time = start_time.replace(microsecond=0, second=0)

	#convert times to timedelta object so they can be subtracted
	end_time = datetime.timedelta(hours = end_time.hour, minutes = end_time.minute, seconds = end_time.second)
	start_time = datetime.timedelta(hours = start_time.hour, minutes = start_time.minute, seconds = start_time.second)
	time_diff = end_time - start_time 
	time_diff = time_diff.total_seconds() 

	#find start and end lane
	end_lane = filtered_dataframe.loc[filtered_dataframe.index[0],'lane']
	start_lane = filtered_dataframe.loc[filtered_dataframe.index[filtered_dataframe.shape[0] - 1],'lane']

	#return start loc, end loc, and time_diff
	output = [start_lane,end_lane,start_time,tag_value,time_diff,trx_time]
	return output

#Function to open files and export trips, references other functions
def process_detailed_trans_file(filename):
	new_filename = 'Trips_' + filename 
	df = open_xls_405(filename) #create dataframes
	print 'File Import Complete'
	tags = gettags(df) #tags from imported file
	Trips_Output =[]

	print 'Start Trip Loop'
	for tag in tags:
		tag_index_value = tag_index(df, tag) #list of rows matching tag
		tag_df = match_tag_extract(tag_index_value, df)
		trip = create_trips(tag_df)
		if trip [4] == 0:
			continue
		elif trip [4] >= 7200:
			continue
		else:
			Trips_Output.append(trip)
			print '.',
	print '\n'


	#create Trips_Output_df dataframe
	print 'creating output dataframe'
	Trips_Output_df = pd.DataFrame(Trips_Output) #create dataframe from trips
	Trips_Output_df.columns = ['Start_loc','End_loc','start_time','tag_num','Time_sec','Trx_Time'] #rename columns
	Trips_Output_df['Trip'] = Trips_Output_df['Start_loc'] + Trips_Output_df['End_loc'] #group origin/destination pairs

	#create SB01 - SB10 trips
	df_SB01SB10 = Trips_Output_df[Trips_Output_df.Trip == 'SB01SB10']
	df_SB01SB10 =  df_SB01SB10.sort_values('start_time', ascending = True) #Filter Travel Times 
	df_SB01SB10['Time_sec'] = medfilt(df_SB01SB10['Time_sec'], 5) #5 is the filter size window

	#create SB01 - SB05 trips
	df_SB01SB05 = Trips_Output_df[Trips_Output_df.Trip == 'SB01SB05']
	df_SB01SB05 =  df_SB01SB05.sort_values('start_time', ascending = True) #Filter Travel Times 
	df_SB01SB05['Time_sec'] = medfilt(df_SB01SB05['Time_sec'], 5) #5 is the filter size window

	#create NB01 - NB10 trips
	df_NB01NB10 = Trips_Output_df[Trips_Output_df.Trip == 'NB01NB10']
	df_NB01NB10 =  df_NB01NB10.sort_values('start_time', ascending = True)
	df_NB01NB10['Time_sec'] = medfilt(df_NB01NB10['Time_sec'], 5) #5 is the filter size window

	#create NB08 - NB10 trips
	df_NB08NB10 = Trips_Output_df[Trips_Output_df.Trip == 'NB08NB10']
	df_NB08NB10 =  df_NB08NB10.sort_values('start_time', ascending = True)
	df_NB08NB10['Time_sec'] = medfilt(df_NB08NB10['Time_sec'], 5) #5 is the filter size window

	print 'dataframes for workbook complete'

	##### RE-INDEX CODE ##### STILL WORKING ON AS OF 2/8/2016, NOT COMPLETE
	#new_index = pd.date_range("00:00:00", "23:59:00", freq="1min").time

	#new index and new index variables
	minute = 1 / 1440.
	seconds_per_day = 60 * 60 * 24.
	new_index = np.arange(0,1,minute) #list of new index values

	#create blank dataframe using new_index
	df_blank_values = pd.DataFrame({'blank': pd.Series(np.zeros(1440), index = new_index)})
	df_blank_values['Trx_Time'] = df_blank_values.index.values

	#create df_pre_merge_trips 
	df_pre_merge_trips = df_NB01NB10
	df_pre_merge_trips['Trx_Time'] = (np.round(df_pre_merge_trips['start_time'].dt.seconds / 60) * 60) / seconds_per_day #remove seconds from trip start time, round to minutes
	df_pre_merge_trips = df_pre_merge_trips.groupby('Trx_Time').mean() #average trips by start time to nearest min
	df_pre_merge_trips['Trx_Time'] = df_pre_merge_trips.index.values # set index values to trip start time in minutes

	#combine dataframes
	frames = [df_pre_merge_trips,df_blank_values] #list of frames to merge
	df_merged = pd.concat(frames) #concat frames 
	df_merged = df_merged.sort_values('Trx_Time',ascending=True) #sort merged dataframe by Trx_Time
	df_merged['Time_sec'] = df_merged['Time_sec'].interpolate() #interpolate missing values
	del df_merged['blank'] #remove blank column
	del df_merged['tag_num'] #remove tag number column

	#output to xls
	with pd.ExcelWriter(new_filename) as writer:
		Trips_Output_df.to_excel(writer, sheet_name='trips')
		df_SB01SB10.to_excel(writer, sheet_name='SB01SB10')
		df_SB01SB05.to_excel(writer, sheet_name='SB01SB05')
		df_NB01NB10.to_excel(writer, sheet_name='NB01NB10')
		df_NB08NB10.to_excel(writer, sheet_name='NB08NB10')
		df_merged.to_excel(writer, sheet_name='NB01NB10_interp')

	#Trips_Output_df.to_excel(new_filename, sheet_name='Trips')

#Open trip workbook 
def open_405_trip(filename):
	df = pd.read_excel(filename, header = 4)

	try:
		bill_extract_text = 'Bill'
		time = dec_to_time(df[bill_extract_text].str.slice(10,19)) 
		time_unformatted = df[bill_extract_text].str.slice(10,19) 
	except AttributeError:
		try:
			bill_extract_text = 'Billable'
			time = dec_to_time(df[bill_extract_text].str.slice(10,19)) 
			time_unformatted = df[bill_extract_text].str.slice(10,19) 
		except KeyError:
			bill_extract_text = 'Filtertype'
			time = dec_to_time(df[bill_extract_text].str.slice(10,19)) 
			time_unformatted = df[bill_extract_text].str.slice(10,19) 
	except KeyError:
		try:
			bill_extract_text = 'Billable'
			time = dec_to_time(df[bill_extract_text].str.slice(10,19)) 
			time_unformatted = df[bill_extract_text].str.slice(10,19) 
		except KeyError:
			bill_extract_text = 'Filtertype'
			time = dec_to_time(df[bill_extract_text].str.slice(10,19)) 
			time_unformatted = df[bill_extract_text].str.slice(10,19) 

	trip_list_series = df['Trip Info'].str.rsplit(',', expand = True)[0] #select trip ID
	df['FareID'] = df['Trip Info'].str.rsplit(',', expand = True)[2].str.slice(6,11)
	df['Trip Info'] = trip_list_series #remove all information except trip ID
	transaction_ID = df['Plaza'].str.rsplit(',', expand = True)[1] #extract Transaction ID
	df['Plaza'] =  Plaza_ID = df['Plaza'].str.rsplit(',', expand = True)[0] #replace data w/plaza ID
	df[' TrxID'] =  transaction_ID #replace data with transaction number 
	df['Date'] = df[bill_extract_text].str.slice(0,10) #get date from billing info column
	df['Bill'] = time
	df['Time'] = time_unformatted
	return df

#concert hh.mm.sss to timedelta format, series input
def dec_to_time(time):
	time_delta_list = []
	for item in time:
		timedelta = datetime.timedelta(hours = int(item[0:3]), minutes = int(item[4:6]), seconds = int(item[7:]))
		time_delta_list.append(timedelta)
	timedelta_series = pd.Series(time_delta_list)
	return timedelta_series

#build trips based on tag data
### Dataframe should look like the following: df['Plaza','TAGID','Start_Time','READERTMST']
### TAGID does not include agency, Start_Time is in datetime format, READERTMST is in timedelta format, Plaza is the toll point - no foramt required
def tag_trip_builder(df):
	df['HOSTNAME'] = df['Plaza']
	tag_list = df['TAGID'].drop_duplicates()
	tag_list = tag_list.tolist()
	count = 0

	#create lists for trip building
	trip_desc_list = []
	travel_time_list = []
	start_date_list = []
	trip_tag_list = []

	while len(tag_list) > 0:

		print count
		count += 1
		tag = tag_list[0]
		df_tag = df[df['TAGID'] == tag]
		df_tag = df_tag.sort_values('Start_Time')
		#df_tag['TT_min'] =  df_tag['READERTMST'] - df_tag['READERTMST'].iloc[0]

		counter = 0
		df_tag['TT_min'] = pd.Series(np.nan,df_tag.index) #blank TT series for df_tag dataframe
		previous_index = []
		ten_minute_datetime = datetime.timedelta(minutes=10) #10 minute interval used between two toll points, also splits trips by direction
		for item in df_tag['READERTMST'].iteritems():
			if counter == 0:
				df_tag['TT_min'].loc[item[0]] = df_tag['READERTMST'].loc[item[0]] - df_tag['READERTMST'].loc[item[0]]  
				previous_index = item[0]
				counter += 1
			elif df_tag['READERTMST'].loc[item[0]] - df_tag['READERTMST'].loc[previous_index] < ten_minute_datetime:
				df_tag['TT_min'].loc[item[0]] = df_tag['READERTMST'].loc[item[0]] - df_tag['READERTMST'].loc[previous_index]  
				previous_index = item[0]
				counter += 1
			else:
				break

		#df_tag['TT_min'] =  [df for in df_tag['READERTMST']]
		five_minute_datetime = datetime.timedelta(minutes=10) #10 minute interval used between two toll points, also splits trips by direction
		df_size_before_filter = df_tag.shape[0] #number of rows in dataframe before filter
		df_tag = df_tag[df_tag['TT_min'] <= five_minute_datetime]
		df_size_after_filter =  df_tag.shape[0] #num of rows in dataframe after filter

		#remove tag if no additional trips need to be formed
		if df_size_before_filter - df_size_after_filter == 0:
			tag_list.remove(tag)

		trip_start = df_tag['HOSTNAME'].head(1).iloc[0]
		trip_end = df_tag['HOSTNAME'].tail(1).iloc[0]
		trip_desc = trip_start + trip_end

		travel_time_start = df_tag['READERTMST'].head(1).iloc[0]
		travel_time_end = df_tag['READERTMST'].tail(1).iloc[0]
		travel_time =  datetime.timedelta.total_seconds(travel_time_end - travel_time_start) / 60

		start_date = datetime.timedelta(hours = df_tag['Start_Time'].head(1).iloc[0].hour, minutes = df_tag['Start_Time'].head(1).iloc[0].minute, seconds = df_tag['Start_Time'].head(1).iloc[0].second)

		trip_desc_list.append(trip_desc)
		travel_time_list.append(travel_time)
		start_date_list.append(start_date) 
		trip_tag_list.append(tag) 

		#
		df = df.drop(df_tag.index) #remove tags for formed trip


	export_df = pd.DataFrame({'Start_Time':start_date_list,
				  'Trip_Destination':trip_desc_list,
				  'Tag':trip_tag_list,
				  'Travel_Time':travel_time_list})
	export_df = export_df[export_df['Travel_Time'] > 0]
	#new_index = pd.date_range("00:00:00", "23:59:00", freq="5min").time
	#export_df = export_df.reindex(new_index)
	return export_df 


#use dataframe('Start_Time','Trip_Destination','Tag','Travel_Time')
# to create 5 minute travel time data

def create_travel_time_dataframe(export_df):
	five_minute_datetime = datetime.timedelta(minutes=5) #5 min in datetime dtype
	export_df['Start_Time'] = export_df['Start_Time'].dt.round(five_minute_datetime) #round start time to nearest 5 min interval
	df_tt = pd.pivot_table(export_df,values='Travel_Time',index='Start_Time',columns=['Trip_Destination'],aggfunc=np.median) #create export dataframe using pivot table

	new_index = pd.timedelta_range("00:00:00", "23:59:00", freq="5min")
	df_tt = df_tt.reindex(new_index)

	#interpolate missing values in df_tt dataframe
	for column in df_tt.columns:
		df_tt[column] = df_tt[column].interpolate()	

	return df_tt



#if pickle file present skip others
list_of_files = get_files()
count = 0
for file in list_of_files:
	if 'pickle' in file and count == 0:
		list_of_files = []
		list_of_files.append(file)
		count += 1
	elif 'pickle' in file:
		list_of_files.append(file)
	else:
		continue


def open_detailed_txn_travle_time(file):
	print 'importing     ' + file
	name_re_loc = re.search('\d{8}',file).span() #regular exp to get date from filename
	name_date = file[name_re_loc[0]:name_re_loc[1]] #extract date from filename
	df = pd.read_csv(file) #open file
	df['Date'] = name_date #add date column
	df['Time'] = df['Unnamed: 0'] #create time column
	df = df.drop('Unnamed: 0',1)#drop Unnamed column
	df['Year'] = df['Date'].str.slice(0,4,1)
	#df = df[['NB03NB04','NB03NB05','NB03NB07','NB03NB10','NB01NB04','Time','Date','Year']] #only keep these columns of dataframe
	return df

def remove_old_figures(files):
	for file in files:
		if 'png' in file:
			os.remove(file)
		else:
			continue

def NB_trips_only(files): #input list
	files_modified = []
	for file in files:
		if 'NB' in file:
			files_modified.append(file)
		else:
			continue
	return files_modified

def open_520_duplicate_tags(filename, duplicate_time_value = datetime.timedelta(minutes = 60)):
	problem_tag_list = []
	df = pd.read_csv(filename,header=3)
	df = df.dropna(subset=['Number'])

	#create series of tags with 2 or more daily reads
	series_tags = df['Number'].value_counts().where(df['Number'].value_counts() >= 2) 
	series_tags = series_tags.dropna() 
	list_tags = series_tags.index.tolist()

	print '\nprocessing ' + str(len(list_tags)) + ' tags\n'
	start_tag_script_time = datetime.datetime.now() #to calculate script runtime
	for tag in list_tags:
		print '.',
		df_tag = df[df['Number'] == tag]
		df_tag['Trx DateTime'] = pd.to_datetime(df_tag['Trx DateTime'])
		time_list = df_tag['Trx DateTime'].tolist()

		#calculate variance between tag read time
		#values below threshold append to problem_tag_list
		for i in time_list:
			for j in time_list:
				if abs(i -j) <= duplicate_time_value and abs(i -j) > datetime.timedelta():
					print '*',
					problem_tag_list.append(tag)
	print datetime.datetime.now() - start_tag_script_time
	return problem_tag_list

def open_520_detailed(filename):
	df = pd.read_csv(filename,header=3)
	return df
	df = df.dropna(subset=['Number'])
