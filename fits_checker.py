import astropy.io.votable as vot
import pandas as pd
import json, sys

input_file = '../data/rows_as_votable_1558460885_1297.vot'
output_file = '../data/noao_2016A_soar_goodman_observations.json'

table = vot.parse_single_table(input_file)
table_length = table.nrows

# Desired Information
desired_keys = ['dtpropid','date_obs','dtpi','ra','dec','filter','exposure',
    'obstype','obsmode']

total_output = []

print ""
for i in range(table.nrows):
    if i % 1000 == 0:
        sys.stdout.write("\r{} / {}".format(i,table.nrows))
        sys.stdout.flush()
    entry_dict = {}
    for vot_key in desired_keys:
        value = table.array[vot_key][i]
        if repr(value) == 'masked':
            value = None
        entry_dict[vot_key] = value
    total_output.append(entry_dict)

json.dump(total_output,open(output_file,"w+"))
print "\rConversion Completed."

observations = []
arc_calibrations = []
calibrations = []

for entry in total_output:
    info = {}
    info['date_obs'] = entry['date_obs']
    info['dtpropid'] = entry['dtpropid']
    info['ra'] = entry['ra']
    info['dec'] = entry['dec']
    info['exposure'] = entry['exposure']

    if entry['obstype'].lower() == 'object':
        observations.append(info)
    elif entry['obstype'].lower() == 'comp':
        arc_calibrations.append(info)
    else:
        calibrations.append(info)

json.dump(observations,open("../data/soar-goodman_observations_2016A.json","w+"))
json.dump(arc_calibrations,open("../data/soar-goodman_arc-calibrations_2016A.json","w+"))
json.dump(calibrations,open("../data/soar-goodman_calibrations_2016A.json","w+"))

print "Observations: {}".format(len(observations))
print "Arc-Lamp Calibrations: {}".format(len(arc_calibrations))
print "Calibrations: {}".format(len(calibrations))
