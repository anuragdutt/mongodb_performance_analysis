import os
import sys
import json
import pprint as pp
import numpy as np
import pandas as pd
import re

def get_measurement_folder(s):
	mf = re.sub(r'[^a-zA-Z0-9_ ]+', '', s)
	mf = mf.replace('  ', '')
	mf = mf.replace(' ', '_')
	return mf


os.chdir('D:/Dropbox/Work/stony_brook/independent_study/mongodb/mongodb_performance_results/expanded_metrics/')

def get_measurement_folder(s):
	mf = re.sub(r'[^a-zA-Z0-9_ ]+', '', s)
	mf = mf.replace('  ', '')
	mf = mf.replace(' ', '_')
	return mf


indices = {
	'cedar_perf_result_id': None, 
	'commit': None, 
	'commit_date':['$date', '$numberLong'] , 
	'evg_create_date': ['$date', '$numberLong'],
	'order':['$numberInt'],
	'value':['$numberDouble'],
	'version':None
}

meta_indices = {
	'_id': ['$oid'],
	'lastSuccessfulUpdate': ['$date', '$numberLong'],
	'lastUpdateAttempt': ['$date', '$numberLong'],
	'updateFailures': ['$numberInt'],
}


if __name__ == '__main__':

	in_file = open('time_series.json', 'rb')
	
	for line in in_file:

		table = []
		jline = json.loads(line)
		oid = jline["_id"]['$oid']
		data = jline['data']

		# insert logic to parse meta-data here
		measurement = jline['measurement']
		for row in data:
			flat = [oid]
			for k, v in row.items():
				if k in indices:
					flat.append(v)
				else:
					ixs = indices[k].copy()
					try:
						v_ = v.get(ixs.pop(0))
						while ixs:
							v_ = v_.get(ixs.pop(0))
					except AttributeError:
						v_ = None
					flat.append(v_)
			table.append(flat)

		flat_names = ['oid'] + list(row.keys())
		df = pd.DataFrame(table, columns=flat_names)
		mf = get_measurement_folder(measurement)

		save_path = '/'.join(['tests_segregated', mf, f"perf_{oid}.csv"])
		df.to_csv(save_path, index=False)