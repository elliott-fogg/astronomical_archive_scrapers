import requests
import json
import re
from datetime import datetime

r = requests.get("https://schedule.lco.global/")

variables = re.findall(r"var (\S+) = (.+?);", r.text, re.DOTALL)
v_dict = {v[0]: v[1] for v in variables}

categories = ("sites", "observations", "weatherdata", "sun_up_data", 
              "downtimedata", "sequencer_data")

output_data = {v: json.loads(v_dict[v]) for v in v_dict if v in categories}

output_data["time"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

json.dump(output_data, open("lco_schedule.json", "w"), indent=4)

