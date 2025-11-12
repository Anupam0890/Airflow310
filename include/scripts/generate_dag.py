import os
import json
import shutil
import fileinput

TEMPLATE_FILE = 'include/templates/process_file.py'

for filename in os.listdir('include/data'):
    if filename.endswith('.json'):
        config = json.load(open(f"include/data/{filename}"))
        new_dagfile = f"dags/process_{config['dag_id']}.py"
        shutil.copyfile(TEMPLATE_FILE, new_dagfile)
        for line in fileinput.input(new_dagfile, inplace=True):
            line = line.replace("DAG_ID_HOLDER", config['dag_id'])
            line = line.replace("SCHEDULE_HOLDER", config['schedule'])
            line = line.replace("INPUT_HOLDER", config["input"])
            print(line, end="")

