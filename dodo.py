import os
import sys
import csv

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "5432"
ACTION_FILE = "actions.sql"

def preprocess_workload_csv(workload_csv):
    rows = []
    with open(workload_csv, 'r') as f:
        csvreader = csv.reader(f)
        i = 0
        rows = []
        lastRow = None
        for row in csvreader:
            if row[13].startswith('duration'):
                if i > 0:
                    lastRow[7] = row[7]
                    lastRow[13] = f"{row[13]}  {lastRow[13]}"
                    rows.append(lastRow)                
            elif row[13].startswith('statement'):
                lastRow = row

            i += 1

    with open(workload_csv, 'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(rows)

def run_dexter(workload_csv, action_file):
    out = os.popen(f"cat {workload_csv} | dexter --input-format=csv\
        --min-calls=0 --min-time=0 --min-cost-savings-pct=30 \
        postgresql://{DEFAULT_USER}:{DEFAULT_PASS}@{DEFAULT_HOST}:{DEFAULT_PORT}/{DEFAULT_DB}").read()
    for line in out.split('\n'):
        # example: Index found: public.review (i_id)
        if line.startswith('Index Found:'):
            tokens = line.split()
            table = tokens[2]
            field = tokens[3].strip('()')
            # create index idx_public_review_i_id on public.review (i_id);
            sql = f"create index if not exists idx_{table.replace('.', '_')}_{field} on {table} ({field});"
            os.popen(f"echo {sql} >> {action_file}")

def recommend_actions(workload_csv):
    print(f"Preprocessing {workload_csv} to make it Dexter digestable...")
    preprocess_workload_csv(workload_csv)
    print('Done!')
    print('Running Dexter to get index recommendations...')
    run_dexter(workload_csv, ACTION_FILE)
    print(f"Done! Actions dumped in {ACTION_FILE}")

def set_log_duration(flag):
    value = 0 if flag else -1
    cmd = f"PGPASSWORD={DEFAULT_PASS} psql --host={DEFAULT_HOST} --dbname={DEFAULT_DB} \
        --username={DEFAULT_USER} --command=\"ALTER SYSTEM SET log_min_duration_statement = {value}\""
    os.popen('cmd')

def task_project1():
    """
    Generate actions.
    """
    def main(workload_csv, timeout):
        print(f"dodo received workload CSV: {workload_csv}")
        print(f"dodo received timeout: {timeout}")

        duration_collected = 'duration_collected.tmp'
        if os.path.exists(duration_collected):
            recommend_actions()
            set_log_duration(False)
            os.remove(duration_collected)
        else:
            set_log_duration(True)
            with open(duration_collected, 'w') as f:
                pass

    return {
        "actions": [
            main,
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        "uptodate": [False],
        "verbosity": 2,
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "The PostgreSQL workload to optimize for.",
                "default": None,
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "The time allowed for execution before this dodo task will be killed.",
                "default": None,
            },
        ],
    }

def task_project1_setup():
    """
    Setup dependencies.
    """
    return {
        "actions": [
            'echo "Setting up project dependencies..."',
            'echo "Done!"',
        ],
        "uptodate": [False],
        "verbosity": 2
    }
