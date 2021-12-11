import os
import sys
import json
import pprint as pp
import numpy as np
import pandas as pd
import re
import pprint


os.chdir('D:/mongodb/mongodb_performance_results/expanded_metrics/')

def get_measurement_folder(s):
	mf = re.sub(r'[^a-zA-Z0-9_ ]+', '', s)
	mf = mf.replace('  ', '')
	mf = mf.replace(' ', '_')
	return mf



if __name__ == '__main__':
	
	in_file = open('change_points.json', 'rb')
	count = 0

	measurements_all = []

	master_list = []

	for line in in_file:

		jline = json.loads(line)
		oid = jline["_id"]['$oid']
		
		measurement = jline['time_series_info']['measurement']

		master_list.append([oid, measurement])
		measurements_all.append(measurement)

	master_data = pd.DataFrame(master_list, 
								columns = ['oid', 'measurement'])

	master_data.to_csv("change_points_master.csv")

	df = pd.read_csv("change_points_master.csv")
	df['measurement_folder'] =  df['measurement'].apply(lambda s: get_measurement_folder(s))
	df.to_csv("change_points_master_with_path.csv", index = False)

	measurements_all = df['measurement_folder']

	for m in np.unique(measurements_all):	
		if not os.path.exists(os.path.join('change_points', m)):
			os.mkdir(os.path.join('change_points', m))
