import datetime as dt
import mysql.connector


class Tracker():
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig

    
    def assign_job_id(self):

        # today_dt = dt.date.today() - dt.timedelta(days=1)
        today_dt = dt.date.today()
        today = today_dt.strftime("%Y-%m-%d")
        job_id = self.jobname + "_" + today
        return job_id
 
    
    def update_job_status(self, status):
        job_id = self.assign_job_id()
        print(f"Job ID Assigned: {job_id}")
        update_time = dt.datetime.now()
        db_connection = self.get_db_connection()
        db_cursor = db_connection.cursor()

        try:
            db_cursor.execute(f"INSERT INTO gcp.job_tracker VALUES ('{job_id}', '{status}', '{update_time}');")
            db_cursor.execute("FLUSH TABLES;")
        except mysql.connector.Error as error:
            print(f"Error while executing SQL statement for job tracker table: {error}")   
        return

    def get_job_status(self, job_id):

        db_connection = self.get_db_connection()
        db_cursor = db_connection.cursor()
          
        try:
            
            db_cursor.execute(f"SELECT gcp.job_tracker.Status FROM gcp.job_tracker WHERE gcp.job_tracker.Job_id = '{job_id}';")
            status = db_cursor.fetchone()
            return status[0]
        except mysql.connector.Error as error:
            print(f"Error while executing SQL statement for job tracker table: {error}")
        except TypeError:
            print(f"Job Id '{job_id}' does not exist")
        return


    def get_db_connection(self):
        mysql_connection = None
        try:
            mysql_connection = mysql.connector.connect(**self.dbconfig)
        except mysql.connector.Error as error:
            print(f"Error while connecting to MySQL Server: {error}")
        return mysql_connection


