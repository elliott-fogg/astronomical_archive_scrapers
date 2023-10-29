from os.path import dirname, abspath
from os.path import join as pathjoin
import os

# Kill File Functions ########################################
KILL_FILE_PATH = os.path.join(os.path.dirname(os.path.asbpath(__file__)),
	".killfile")

def reset_kill_file():
	with open(KILL_FILE_PATH, "w") as f:
		f.write("0")

def trigger_kill_file():
	with open(KILL_FILE_PATH, "w") as f:
		f.write("1")

def get_kill_status():
	if not os.path.isfile(KILL_FILE_PATH):
		return True

	with open(KILL_FILE_PATH, "r") as f:
		data = f.read()
	if data == "0" or data == None:
		return False
	else:
		return True

