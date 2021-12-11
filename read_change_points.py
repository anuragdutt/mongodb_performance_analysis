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




def get_measurement_folder(s):
	mf = re.sub(r'[^a-zA-Z0-9_ ]+', '', s)
	mf = mf.replace('  ', '')
	mf = mf.replace(' ', '_')
	return mf


if __name__ =='__main__':

	in_file = open('change_points.json', 'rb')
	# # json_object = json.load(in_file)

	# with open('change_points.json', 'rb') as json_file:
	# 	json_object = json.load(json_file)
	count = 0


	for line in in_file:


		jline = json.loads(line)
		# pprint.pprint(jline)

		oid = jline["_id"]['$oid']

		row = {
		'oid' : oid,
		'cedar_perf_result_id' : jline['cedar_perf_result_id'],
		'algorithm_test_statistic_1' : jline['algorithm']['options'][0]['name'],
		'algorithm_test_value_1' : jline['algorithm']['options'][0]['value'],
		'algorithm_test_statistic_2' : jline['algorithm']['options'][1]['name'],
		'algorithm_test_value_2' : jline['algorithm']['options'][1]['value'],

		'ts_info_measurement' : jline['time_series_info']['measurement'],
		'ts_info_project' : jline['time_series_info']['project'],
		'ts_info_task' : jline['time_series_info']['task'],
		'ts_info_test' : jline['time_series_info']['test'],
		'ts_info_variant' : jline['time_series_info']['variant'],

		'order' : jline['order']['$numberInt'],
		'change_point_percent_change' : jline['order']['$numberInt'],
		'version' : jline['version']
		}

		measurement = jline['time_series_info']['measurement']
		# print(row)
		df = pd.DataFrame(row, index=[0])
		mf = get_measurement_folder(measurement)
		save_path = '/'.join(['change_points', mf, f"perf_{oid}.csv"])
		
		df.to_csv(save_path, index=False)
		count += 1

		# if count == 1:
		# 	pprint.pprint(jline)
		# 	break

		if count % 1000 == 0:
			print(count)