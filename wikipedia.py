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
LOOKBEHIND_DAYS = 1
LOOKBEHIND_HOURS = LOOKBEHIND_DAYS * 24
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
				print status_dict
			except ValueError:
				status_dict = ZERO_STATE
	except IOError:
		status_dict = ZERO_STATE

	# backtrack = sys.argv[1]
	
	# backtrack = backtrack.split('=')[1]

	# if backtrack == 'True':
	# 	backtrack = True
	# else:
	# 	backtrack = False

	now = datetime.now()
	goal_date = now - timedelta(days=LOOKBEHIND_DAYS)

	total_hours = LOOKBEHIND_HOURS
	current_date = now - timedelta(hours=12)

	while total_hours > 0:
		current_url = ROOT_URL.format(
			year=current_date.year,
			month=str(current_date.month).zfill(2),
			day=str(current_date.day).zfill(2),
			hour=str(current_date.hour).zfill(2)
		)

		

		print current_url
		filename = current_url.split('/')[-1]

		# UNCOMMMENT TO DOWNLOAD
		# ===
		result = os.system("wget {}".format(current_url))

		if result == 0:
			# os.system("java -jar {} {}".format(JAR_NAME, filename))
			# os.system("rm -rf {}".format(filename))
			status_dict = update_status(
				status_dict,
				month=current_date.month,
				day=current_date.day,
				year=current_date.year,
				hour=current_date.hour
			)
		else:
			quit()
			
		total_hours -= 1
		current_date = current_date - timedelta(hours=1)
