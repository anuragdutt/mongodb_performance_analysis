import os
import sys
import json
import pprint as pp
import numpy as np
import pandas as pd

os.chdir('D:/Dropbox/Work/stony_brook/independent_study/mongodb/mongodb_performance_results/expanded_metrics/')

if __name__ == '__main__':


	in_file = open('time_series.json', 'rb')

	measurement_all = []
	master_list = []

	for line in in_file:

		table = []
		jline = json.loads(line)
		oid = jline["_id"]['$oid']
		measurement = jline['measurement']
		project = jline['project']
		task = jline['task']
		test = jline['test']
		try: 
			last_successful_update = jline['lastSuccessfulUpdate']['$date']['$numberLong']
		except KeyError:
			last_successful_update = None

		try:
			last_update_attempt = jline['lastUpdateAttempt']['$date']['$numberLong']
		except KeyError:
			last_update_attempt = None
		try:
			update_failures = jline['updateFailures']['$numberInt']
		except KeyError:
			update_failures = None

		data = jline['data']
		
		master_list.append([oid, measurement, project, task, test, last_successful_update, last_update_attempt, update_failures])

		measurement_all.append(measurement)

		

	master_data = pd.DataFrame(master_list, 
								columns = ['oid', 'measurement', 'project', 'task', 'test', 'last_successful_update', 'last_update_attempt', 'update_failures'])
	master_data.to_csv("master_data.csv", index = False)



