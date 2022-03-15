import os
import sys
import csv

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "5432"
ACTION_FILE = "actions.sql"

def preprocess_workload_csv(workload_csv, processed_csv):
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

    with open(processed_csv, 'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(rows)

def run_dexter(workload_csv, action_file):
    out = os.popen(f"cat {workload_csv} | dexter --input-format=csv\
        --min-calls=0 --min-time=0 --min-cost-savings-pct=30 \
        postgresql://{DEFAULT_USER}:{DEFAULT_PASS}@{DEFAULT_HOST}:{DEFAULT_PORT}/{DEFAULT_DB}").read()
    for line in out.split('\n'):
        # example: Index found: public.review (i_id)
        if line.startswith('Index found:'):
            tokens = line.split()
            table = tokens[2]
            field = tokens[3].strip('()')
            # create index idx_public_review_i_id on public.review (i_id);
            sql = f"\"create index if not exists idx_{table.replace('.', '_')}_{field} on {table} ({field});\""
            os.popen(f"echo {sql} >> {action_file}")
    # DEBUG
    dexter_log = 'dexter.log'
    with open(dexter_log, 'a') as f:
        f.write(out)
        f.write('\n\n')
        f.write('================ End Running Dexter ===============')
        f.write('\n\n')

def recommend_actions(workload_csv):
    print(f"Preprocessing {workload_csv} to make it consumable to Dexter...")
    processed_workload = f"{workload_csv}_processed"
    preprocess_workload_csv(workload_csv, processed_workload)
    print('Done!')
    print('Running Dexter to get index recommendations...')
    run_dexter(processed_workload, ACTION_FILE)
    print(f"Done! Actions dumped in {ACTION_FILE}")

def get_psql_command(sql):
    return f"PGPASSWORD={DEFAULT_PASS} psql --host={DEFAULT_HOST} --dbname={DEFAULT_DB} --username={DEFAULT_USER} --command=\"{sql}\""

def set_log_duration(flag):
    value = 0 if flag else -1
    print(f"Setting postgres log_min_duration_statement = {value}")
    cmd = get_psql_command(f"ALTER SYSTEM SET log_min_duration_statement = {value}")
    os.popen(cmd)
    # don't have to reload postgres because testing script will do it later

def task_project1():
    """
    Generate actions.
    """
    def main(workload_csv, timeout):
        print(f"dodo received workload CSV: {workload_csv}")
        print(f"dodo received timeout: {timeout}")
        if not os.path.exists(ACTION_FILE):
            with open(ACTION_FILE, 'w') as f:
                pass

        duration_collected = 'duration_collected.tmp'
        if os.path.exists(duration_collected):
            print(f"{duration_collected} exists!")
            recommend_actions(workload_csv)
            set_log_duration(False)
            print(f"Removing {duration_collected}")
            os.remove(duration_collected)
        else:
            print(f"{duration_collected} doesn't exists")
            set_log_duration(True)
            print(f"Creating {duration_collected}")
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
    def remove_db_indexes():
        sql = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename, indexname"
        get_indexes_cmd = get_psql_command(sql)
        out = os.popen(get_indexes_cmd).read()
        for line in out.split():
            if line.startswith('idx_'):
                os.popen(get_psql_command(f"drop index if exists {line};"))
    return {
        "actions": [
            'echo "Removing db indexes..."',
            remove_db_indexes,
            'echo "Done!"',
            'echo "Setting up project dependencies..."',
            'echo "Done!"',
        ],
        "uptodate": [False],
        "verbosity": 2
    }
