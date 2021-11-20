import os
import sys
import numpy as np
import pandas as pd
import re

os.chdir('D:/Dropbox/Work/stony_brook/independent_study/mongodb/mongodb_performance_results/expanded_metrics/')

def get_measurement_folder(s):
	mf = re.sub(r'[^a-zA-Z0-9_ ]+', '', s)
	mf = mf.replace('  ', '')
	mf = mf.replace(' ', '_')
	return mf


if __name__ == '__main__':

	df = pd.read_csv("master_data.csv")
	df['measurement_folder'] =  df['measurement'].apply(lambda s: get_measurement_folder(s))
	df.to_csv("master_data_with_path.csv", index = False)

	measurements_all = df['measurement_folder']

	for m in np.unique(measurements_all):	
		if not os.path.exists(os.path.join('tests_segregated', m)):
			os.mkdir(os.path.join('tests_segregated', m))
