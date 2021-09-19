import transaction_parser as par
import transaction_loader as loa
import transaction_analyzer as ana
import helpers as h
import sys

sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
import configocp

# MySQL connection setup

u = configocp.db_u
p = configocp.db_p
db = configocp.db_s
host = configocp.db_ho
port = configocp.db_po

config = {
    'user': u,
    'password': p,
    'database': db,
    'host': host,
    'port': port,    
 }

def reporter(config, spark_job, module):
    
    tracker = h.Tracker(spark_job, config)

    try:
        module()
        tracker.update_job_status("success")
    except Exception as error:
        print(f"Error while running spark_job '{spark_job}': {error}")
        tracker.update_job_status("failed")
    return

reporter(config, 'transaction_parser', par.parser)
reporter(config, 'transaction_loader', loa.loader)
reporter(config, 'transaction_analyzer', ana.analyzer)
