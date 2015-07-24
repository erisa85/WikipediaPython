#!/usr/local/bin/python
from __future__ import unicode_literals, with_statement

import sys
import os
import json
from datetime import datetime, timedelta

ZERO_STATE = {
	'last_downloaded_month': None,
	'last_downloaded_day': None,
	'last_downloaded_hour': None,
	'last_downloaded_year': None
}

JAR_NAME = 'wikipediapageview_2.11-1.0.jar'
LOOKBEHIND_DAYS = 30
ROOT_URL = "http://dumps.wikimedia.org/other/pagecounts-all-sites/{year}/{year}-{month}/pagecounts-{year}{month}{day}-{hour}0000.gz"

STATUS_FILE_NAME = 'status.json'


def update_status(status, month=None, day=None, year=None, hour=None):
	status['last_downloaded_month'] = month
	status['last_downloaded_day'] = day
	status['last_downloaded_year'] = year
	status['last_downloaded_hour'] = hour
	## SAVE TO FILE

	with open(STATUS_FILE_NAME, "w+") as f:
		json.dump(status, f)

	return status

if __name__ == '__main__':


	try:
		with open(STATUS_FILE_NAME, "r") as f:
			try:
				status_dict = json.load(f)
				print(status_dict)
			except ValueError:
				status_dict = ZERO_STATE
	except IOError:
		status_dict = ZERO_STATE

	now = datetime.now()

	if status_dict == ZERO_STATE:
		start_date = now - timedelta(days=LOOKBEHIND_DAYS)
	else:
		start_date = datetime(year=status_dict['last_downloaded_year'], month=status_dict['last_downloaded_month'], day=status_dict['last_downloaded_day'], hour=1+status_dict['last_downloaded_hour'])
	current_date = now - timedelta(hours=2) #to be sure the data is available to be downloaded we go 2 hours back

	while start_date <= current_date:
		current_url = ROOT_URL.format(
			year=start_date.year,
			month=str(start_date.month).zfill(2),
			day=str(start_date.day).zfill(2),
			hour=str(start_date.hour).zfill(2)
		)
		print(current_url)
		filename = current_url.split('/')[-1]
		print(filename)
		# wget -N ===> overwrites the file if it exists
		result = os.system("wget -N {}".format(current_url))
		if result == 0:
			#copy input file from local to hdfs
			os.system("ephemeral-hdfs/bin/hadoop dfs -rmr {}".format(filename))
			status = os.system("ephemeral-hdfs/bin/hadoop dfs -copyFromLocal {} {}".format(filename, filename))
			if status == 0:
				#to run the jar on spark on your local: spark-submit --class Wikipedia --master local[4] wikipedia-1_0.jar /path/to/input/file
				status = os.system("spark-submit --class wiki.Wikipedia --master {} {} {}".format(sys.argv[1], sys.argv[2], filename))
				if status == 0:
					os.system("rm -rf {}".format(filename))
					os.system("ephemeral-hdfs/bin/hadoop dfs -rmr -skipTrash {}".format(filename))
					status_dict = update_status(status_dict,month=start_date.month,day=start_date.day,year=start_date.year,hour=start_date.hour)
				else:
					quit()
			else:
				quit()
		else:
			quit()
		start_date = start_date + timedelta(hours=1)
