from decouple import config
import requests
import ibm_db
import datetime 
from datetime import datetime, timedelta
import pandas as pd
from exchangelib import (
    IMPERSONATION,
    OAUTH2,
    Account,
    Configuration,
    HTMLBody,
    Identity,
    FileAttachment
)
from exchangelib import Message as OutlookMessage
from exchangelib import OAuth2Credentials, Version
from exchangelib.version import EXCHANGE_O365
from email.message import EmailMessage
import os
import logging
import ibm_db_dbi
from tmw_db2 import TMW_DB2
import warnings
# Define database connection parameters using environment variables
#dsn=f"DATABASE={config('DATABASE')};HOSTNAME={config('HOSTNAME')};PORT={config('PORT')};PROTOCOL={config('PROTOCOL')};UID={config('UID')};PWD={config('PWD')};AUTHENTICATION=SERVER;"
#conn = ibm_db.connect(dsn,"","")
#con_dbi = ibm_db_dbi.Connection(conn)
warnings.simplefilter("ignore")
conn = TMW_DB2(config('UID'), config('PWD'))
# Get authorization key and subdomain from environment variables
authorization_key = config('authorization_key')
SUBDOMAIN = config('SUBDOMAIN')
# Define headers for API requests
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {authorization_key}"
}

# Configuring OAuth2 credentials for email operations
credentials = OAuth2Credentials(
client_id=config('CLIENT_ID'),
client_secret=config('CLIENT_SECRET'),
tenant_id=config('TENANT_ID'),
identity=Identity(primary_smtp_address=config('EMAIL_USER')),)

# Configuring the email account with OAuth2 credentials and Exchange settings
configuration = Configuration(credentials=credentials,auth_type=OAUTH2,server=config('EMAIL_HOST'),version=Version(EXCHANGE_O365),)
account = Account(primary_smtp_address=config('EMAIL_USER'),config=configuration,access_type=IMPERSONATION,autodiscover=False,)

# Check if the database connection is successful
if conn:
    data_to_excel = []
    shema_name = "TMWIN"
    
    # Define a SQL query to select data from the database
    select_sql = f"SELECT * from {shema_name}.RPT_COMM_RATES_ELI"
    
     # Execute the SQL query
    stmt = TMW_DB2.execute_query(conn, select_sql)
    #ibm_db.exec_immediate(conn, select_sql) 
    rows = []
    rows3 = []
    
     # Fetch and store the results from the database
    result = ibm_db.fetch_assoc(stmt)
    
    while result:
        rows.append(result)
        result = ibm_db.fetch_assoc(stmt)
    #print(rows)
    # Get a list of BambooHR employees using an API request
    response = requests.get(f"https://api.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/directory", headers=headers)
    employee = response.json()['employees']
    
    changed_data = []
    # Iterate through each employee
    for emp in employee:
        # Get the ID of the employee for further API requests
        employeeID = emp['id']
        
        
        # Get custom commission data for the employee from BambooHR
        resp = requests.get(f"https://{SUBDOMAIN}.bamboohr.com/api/gateway.php/{SUBDOMAIN}/v1/employees/{employeeID}/tables/customCommission", headers=headers)
        existingdataCS = resp.json()
        
        # Check if there is existing data for the employee
        if existingdataCS != []:
            for existing in existingdataCS:
                
                # Construct a SQL query to search for corresponding data in the database
                research = f"""SELECT RATE, EFFECTIVE, TYPE, CLASS, SITE_NAME, USER_ID, POOL, MULTIPLIER  from {shema_name}.RPT_COMM_RATES_ELI
                WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}') AND (TYPE = '{existing['customType1']}')
                AND (CLASS = '{existing['customClass']}') AND (SITE_NAME = '{existing['customSite']}') AND (USER_ID = '{existing['customTMWUserID']}')
                """
                #stmt2 = ibm_db.exec_immediate(conn, research)
                stmt2 = TMW_DB2.execute_query(conn,research)
                rows2 = []
                
                # Check if any of the constraint fields is null and display them for rectification
                if existing['customEffectiveDate2'] is None or existing['customTMWUserID'] is None or existing['customSite'] is None or existing['customClass'] is None or existing['customType1'] is None:
                    print("aucune recherche possible, veuillez rectifier le(s) champ(s) vide")
                    print("effective date:", existing['customEffectiveDate2'])
                    print("USER_ID:", existing['customTMWUserID'])
                    print("SITE:", existing['customSite'])
                    print("CLASS:", existing['customClass'])
                    print("Type:", existing['customType1'])
                    print("RATE:", existing['customRate'])
                else:
                    result2 = ibm_db.fetch_assoc(stmt2)
                    while result2:
                        rows2.append(result2)
                        result2 = ibm_db.fetch_assoc(stmt2)
                        
                    #When data exist in the database 
                    for r in rows2:
                        print(existing['customTMWUserID'])
                        effective_date = datetime.strptime(existing['customEffectiveDate2'], "%Y-%m-%d").date()
                        rate = r['RATE']
                        rateBhr = float(existing['customRate'])
                        multiplier = float(existing['customMultiplier'])
                        
                        
                        # Verify if rates are identical
                        if rate == rateBhr and r['EFFECTIVE'] == effective_date and r['TYPE'] == existing['customType1'] and r['CLASS'] == existing['customClass'] and r['SITE_NAME'] == existing['customSite'] and r['USER_ID'] == existing['customTMWUserID']:
                            print("les deux rate sont identiques, pas de mise à jour")
                            
                        # Check if rates are different and update the rate
                        if rate != rateBhr and r['EFFECTIVE'] == effective_date and r['TYPE'] == existing['customType1'] and r['CLASS'] == existing['customClass'] and r['SITE_NAME'] == existing['customSite'] and r['USER_ID'] == existing['customTMWUserID'] and r['MULTIPLIER'] == multiplier:
                            update_sql = f"""
                            UPDATE {shema_name}.RPT_COMM_RATES_ELI
                            SET RATE = {rateBhr}
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}')
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            """
                            TMW_DB2.execute_query(conn, update_sql)
                            #ibm_db.exec_immediate(conn, update_sql)
                            print("rate mise à jour")
                            
                            # Define an SQL query that selects specific columns from a database table.
                            # This table is in a schema named in the variable 'shema_name'.
                            # The WHERE clause filters the records based on multiple conditions,
                            # which involve comparing database fields to values in a dictionary named 'existing', and variables 'rateBhr' and 'multiplier'.
                            research2 = f"""SELECT RATE, EFFECTIVE, TYPE, CLASS, SITE_NAME, USER_ID, POOL, MULTIPLIER, MULTIPLIER_EFFECTIVE, END_DATE, INS_TIMESTAMP
                            from {shema_name}.RPT_COMM_RATES_ELI
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}') 
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            AND (RATE = {rateBhr})
                            AND (MULTIPLIER = {multiplier})
                                """
                                
                            # Execute the SQL query using the IBM DB2 connection 'conn'.
                            # The result of the query is stored in the 'stmt3' variable.
                            stmt3 = TMW_DB2.execute_query(conn, research2)
                            #stmt3 = ibm_db.exec_immediate(conn, research2)
                            # Fetch the first row from the result of the SQL query.
                            # This row is returned as a dictionary where the column names are the keys.
                            changed_data = ibm_db.fetch_assoc(stmt3)
                            while changed_data:
                                # For the current row, add a new key-value pair ('status', "Modify") to the dictionary.
                                # This likely indicates that the record has been modified
                                changed_data['status']= "Modify"
                                
                                # Append the current row's dictionary, which now includes the 'status' key, to the 'rows3' list.
                                # This list is probably used later in the code to process these records.
                                rows3.append(changed_data)
                                
                                # Fetch the next row from the result set. If there are no more rows, 'changed_data' will be None,
                                # and the while loop will exit.
                                changed_data = ibm_db.fetch_assoc(stmt3)
                                #rows3.append(["Status:", status])
                                
                        # Check if multipliers are different and update multiplier
                        if rate == rateBhr and r['EFFECTIVE'] == effective_date and r['TYPE'] == existing['customType1'] and r['CLASS'] == existing['customClass'] and r['SITE_NAME'] == existing['customSite'] and r['USER_ID'] == existing['customTMWUserID'] and r['MULTIPLIER'] != multiplier:
                            update_sql = f"""
                            UPDATE {shema_name}.RPT_COMM_RATES_ELI
                            SET MULTIPLIER = {multiplier}
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}')
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            """
                            TMW_DB2.execute_query(conn, update_sql)
                            #ibm_db.exec_immediate(conn, update_sql)
                            print("multiplier mise à jour")
                            
                            # Define an SQL query string that selects specific columns from a table within a schema.
                            # This query filters the results based on several conditions, which are compared to
                            # values stored in the 'existing' dictionary, 'rateBhr', and 'multiplier' variables
                            research2 = f"""SELECT RATE, EFFECTIVE, TYPE, CLASS, SITE_NAME, USER_ID, POOL, MULTIPLIER, MULTIPLIER_EFFECTIVE, END_DATE, INS_TIMESTAMP
                            from {shema_name}.RPT_COMM_RATES_ELI
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}') 
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            AND (RATE = {rateBhr})
                            AND (MULTIPLIER = {multiplier})
                                """
                            
                            # Execute the SQL query using the ibm_db library's exec_immediate function, which sends the query
                            # to the connected database and returns a statement handle to be used for further operations like fetching results.
                            #stmt3 = ibm_db.exec_immediate(conn, research2)
                            stmt3 = TMW_DB2.execute_query(conn, research2)
                            # Fetch the first row from the query result as an associative array (similar to a dictionary).
                            changed_data = ibm_db.fetch_assoc(stmt3)
                            # Loop over the query results. This loop will continue until no more data is returned by fetch_assoc,
                            # which would set changed_data to None and terminate the loop.
                            while changed_data:
                                # Add a new key-value pair ('status', "Modify") to the associative array representing the current row.
                                # This indicates that the record has been modified.
                                changed_data['status']= "Modify"
                                # Append the associative array with the newly added 'status' key to the rows3 list.
                                # The rows3 list seems to be used to collect and store modified records.
                                rows3.append(changed_data)
                                
                                 # Fetch the next row from the query result to continue or terminate the loop.
                                changed_data = ibm_db.fetch_assoc(stmt3)
                            
                        # Check if rates and multipliers are different and update both
                        if rate != rateBhr and r['EFFECTIVE'] == effective_date and r['TYPE'] == existing['customType1'] and r['CLASS'] == existing['customClass'] and r['SITE_NAME'] == existing['customSite'] and r['USER_ID'] == existing['customTMWUserID'] and r['MULTIPLIER'] != multiplier:
                            # Formulate an SQL UPDATE statement to update specific records within a table.
                            # The updates are applied to the MULTIPLIER and RATE fields where the record matches 
                            # the specified criteria.
                            update_sql = f"""
                            UPDATE {shema_name}.RPT_COMM_RATES_ELI
                            SET MULTIPLIER = {multiplier}, RATE = {rateBhr}
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}')
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            """
                            
                            # Execute the UPDATE statement using the database connection 'conn'.
                            TMW_DB2.execute_query(conn, update_sql)
                            #ibm_db.exec_immediate(conn, update_sql)
                            print("multiplier and rate updated")
                            
                            # Construct a SELECT SQL statement to retrieve records from the database that match the update criteria.
                            research2 = f"""SELECT RATE, EFFECTIVE, TYPE, CLASS, SITE_NAME, USER_ID, POOL, MULTIPLIER, MULTIPLIER_EFFECTIVE, END_DATE, INS_TIMESTAMP
                            from {shema_name}.RPT_COMM_RATES_ELI
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}') 
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            AND (RATE = {rateBhr})
                            AND (MULTIPLIER = {multiplier})
                                """
                            
                            # Execute the SELECT statement to get the updated records.
                            #stmt3 = ibm_db.exec_immediate(conn, research2)
                            stmt3 = TMW_DB2.execute_query(conn, research2)
                            # Fetch the first row of the result set as an associative array (similar to a dictionary in Python).
                            changed_data = ibm_db.fetch_assoc(stmt3)
                            while changed_data:
                                # Add a new key 'status' with the value "Modify" to each record to indicate the record has been modified.
                                changed_data['status']= "Modify"
                                # Append the modified record to the 'rows3' list, which presumably is used to track changes or further processing.
                                rows3.append(changed_data)
                                # Fetch the next row of the result set for the next iteration of the loop.
                                changed_data = ibm_db.fetch_assoc(stmt3)
                            
                    # If no data exists in the database, insert a new entry
                    if rows2 == []:
                        # Extract necessary information from the BambooHR data
                        multiplier = float(existing['customMultiplier'])
                        rateBhr = float(existing['customRate'])
                        
                        # Convert the effective date bamboorh in date format
                        effective_date = datetime.strptime(existing['customEffectiveDate2'], "%Y-%m-%d").date()
                        
                        # Calculate the precedent date (one day before the effective date)
                        precedent_dateend = effective_date - timedelta(days=1)
                        
                        # Print some information for debugging or logging
                        print(effective_date, precedent_dateend)
                        print("new entry")
                        
                        # Define an SQL query to update the END_DATE of the most recent entry for the same employee
                        update_sql = f"""
                            UPDATE {shema_name}.RPT_COMM_RATES_ELI
                            SET END_DATE = '{precedent_dateend}'
                            WHERE USER_ID = '{existing['customTMWUserID']}'
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}')
                            AND (POOL = '{existing['customPooledornon-pooled']}')
                            ORDER BY EFFECTIVE DESC LIMIT 1
                            """
                        
                        # Execute the update SQL query to set the END_DATE for the previous entry
                        #ibm_db.exec_immediate(conn, update_sql)
                        TMW_DB2.execute_query(conn, update_sql)
                        rows_affected = ibm_db.num_rows(TMW_DB2.execute_query(conn, update_sql))
                         # Define an SQL query to insert a new entry with the obtained custom commission data
                        insert_sql= f"""
                            INSERT INTO {shema_name}.RPT_COMM_RATES_ELI(EFFECTIVE,TYPE,CLASS,SITE_NAME,USER_ID,RATE,MULTIPLIER,
                            MULTIPLIER_EFFECTIVE,POOL,END_DATE) 
                            VALUES ('{existing['customEffectiveDate2']}',
                                    '{existing['customType1']}',
                                    '{existing['customClass']}',
                                    '{existing['customSite']}',
                                    '{existing['customTMWUserID']}',
                                    '{existing['customRate']}',
                                    '{existing['customMultiplier']}',
                                    '2020-11-30',
                                    '{existing['customPooledornon-pooled']}',
                                    '2030-01-01')
                                """
                        # Execute the insert SQL query to add the new entry to the database
                        #ibm_db.exec_immediate(conn, insert_sql)
                        TMW_DB2.execute_query(conn, insert_sql)
                        
                        # Construct an SQL query string to select specific fields from a table within a given schema.
                        # The WHERE clause filters records that match the specified criteria, using variables
                        research2 = f"""SELECT RATE, EFFECTIVE, TYPE, CLASS, SITE_NAME, USER_ID, POOL, MULTIPLIER, MULTIPLIER_EFFECTIVE, END_DATE, INS_TIMESTAMP
                            from {shema_name}.RPT_COMM_RATES_ELI
                            WHERE (EFFECTIVE = '{existing['customEffectiveDate2']}')
                            AND (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}') 
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            AND (RATE = {rateBhr})
                            AND (MULTIPLIER = {multiplier})
                            AND (END_DATE = '2030-01-01')
                                """
                        
                        # Execute the SQL query using the connection object 'conn' and store the result in 'stmt3'.
                        #stmt3 = ibm_db.exec_immediate(conn, research2)
                        stmt3 = TMW_DB2.execute_query(conn, research2)
                        # Fetch the first row of the result set as an associative array (similar to a dictionary in Python).
                        changed_data = ibm_db.fetch_assoc(stmt3)
                        
                        # Use a while loop to iterate over the result set.
                        while changed_data:
                            # For each row fetched, add a new key 'status' with the value "New Entry" to indicate the row is a new entry.
                            changed_data['status']= "New Entry"
                            
                            # Append the modified row data to the list 'rows3', which will contain all processed records.
                            rows3.append(changed_data)
                            
                            # Fetch the next row of the result set, if any, to continue the loop.
                            changed_data = ibm_db.fetch_assoc(stmt3)
                            
                        # If the previous update operation affected more than 0 rows
                        if rows_affected > 0:
                            
                            # Define an SQL query to select various columns from a specific table
                            # Filter the rows based on several conditions that match 'existing' dictionary values
                            # and a predefined 'precedent_dateend'. Orders the result by the 'EFFECTIVE' column
                            # in descending order and limits the result to 1 row
                            research_prec = f"""SELECT RATE, EFFECTIVE, TYPE, CLASS, SITE_NAME, USER_ID, POOL, MULTIPLIER, MULTIPLIER_EFFECTIVE, END_DATE, INS_TIMESTAMP
                            from {shema_name}.RPT_COMM_RATES_ELI
                            WHERE 
                            (TYPE = '{existing['customType1']}')
                            AND (CLASS = '{existing['customClass']}') 
                            AND (SITE_NAME = '{existing['customSite']}') 
                            AND (USER_ID = '{existing['customTMWUserID']}')
                            AND (END_DATE = '{precedent_dateend}')
                            AND (POOL = '{existing['customPooledornon-pooled']}')
                            ORDER BY EFFECTIVE DESC LIMIT 1
                                """
                            
                            # Execute the SQL query using the ibm_db exec_immediate function
                            #stmt3_prec = ibm_db.exec_immediate(conn, research_prec)
                            stmt3_prec = TMW_DB2.execute_query(conn, research_prec)
                            #Fetch the associative array (dictionary) of the result of the query
                            changed_data_prec = ibm_db.fetch_assoc(stmt3_prec)
                            
                            # Loop through the results of the query
                            while changed_data_prec:
                                # Assign a status of "Modify" to the current row's data
                                changed_data_prec['status'] = "Modify"
                                # Append the modified row data to the rows3 list
                                rows3.append(changed_data_prec)
                                # Fetch the next row of the result set, if any
                                changed_data_prec = ibm_db.fetch_assoc(stmt3_prec)
                                
                                
    # Loop over each record in rows3
    for rs in rows3:
        # Append a list of values from the current record to the data_to_excel list
        # Each value corresponds to a column in the eventual Excel file
        data_to_excel.append(
            [ rs['EFFECTIVE'],  # Effective date
              rs['TYPE'],       # Type
              rs['CLASS'],      # Class
              rs['SITE_NAME'],  # Site name
              rs['USER_ID'],    # User ID
              rs['POOL'],       # Pool
              rs['RATE'],       # RATE
              rs['MULTIPLIER'], # Multiplier value
              rs['MULTIPLIER_EFFECTIVE'], # Effective date of the multiplier
              rs['END_DATE'],   # End date
              rs['INS_TIMESTAMP'],  # Timestamp of insertion or modification
              rs['status'],         # Status
            ]
        )
    if data_to_excel != []:
        # Create a pandas DataFrame from the data_to_excel list
        # Assign column names to the DataFrame
        df = pd.DataFrame(data_to_excel, columns=['Effective', 'Type','CLASS', 'SITE_NAME', 'USER_ID', 'POOL','RATE', 'MULTIPLIER', 'MULTIPLIER_EFFECTIVE','END_DATE','INS_TIMESTAMP','status'])
    
        # Write the DataFrame to an Excel file without the index
        df.to_excel("RPT_COMM_RATES_ELI Update.xlsx", index=False)
    
        # HTML content for the email body, including both French and English sections
        corps_email = "<html><body><p><i><strong>Version française – English below</strong></i></p><p>La table RPT_COMM_RATES_ELI a été mise à jour.</p><p>Veuillez vérifier que les données du fichier joint sont conformes.</p><p>Pour toutes questions, veuillez contacter <a href=mailto:integrationteam@shipenergy.com>integrationteam@shipenergy.com</a>. <span style='color:red;'><strong>NE PAS RÉPONDRE À CE COURRIEL.</strong></span></p><p>Merci</p>"
        corps_email += "<p><i><strong>English version</strong></i></p><p>The RPT_COMM_RATES_ELI table has been updated.</p><p>Please verify that the data in the attached file is accurate.</p><p>For any questions, please contact <a href=mailto:integrationteam@shipenergy.com>integrationteam@shipenergy.com</a>. <span style='color:red;'><strong>DO NOT REPLY TO THIS EMAIL.</strong></span></p><p>Thank you</p></body></html>"
    
        # Create an email message with the OutlookMessage class, specifying account, subject, body, and recipients
        message = OutlookMessage(
            account=account,    # The email account to send from
            subject="RPT_COMM_RATES_ELI Update / Mise à jour",  # The subject of the email
            body=HTMLBody(corps_email),     # The HTML body of the email
            to_recipients=[config("RECIPIENT")])    # The recipient(s) of the email
    
        # Check if the Excel file exists
        if os.path.isfile("RPT_COMM_RATES_ELI Update.xlsx"):
        
            # Open the Excel file in binary read mode
            with open("RPT_COMM_RATES_ELI Update.xlsx", 'rb') as f:
                file_content = f.read()
        
        # Create a file attachment with the content read from the file
            attachment= FileAttachment(name=os.path.basename("RPT_COMM_RATES_ELI Update.xlsx"), content=file_content)
        
        # Attach the file to the message
            message.attach(attachment)
        
         # Send the email message with the attachment
            message.send()
    else:
        print("No entry Modifying or new entry data")
    #close the database             
    #ibm_db.close()
else:
    print("connection failed")
    
try:
    requests.get(
        'https://health.shipenergy.com/ping/d03c2fb2-a07d-40bc-947e-7114aa4c7f98',
        timeout=10
                 )
except requests.RequestException as e:
    logging.debug("Failed to ping %s" % e)