# Commission Schedule Rate Update

## Name
Custom Commission Data Synchronization

## Description
This Python script is designed to synchronize custom commission data between an IBM Db2 database and BambooHR using their respective APIs. It performs data retrieval, comparison, updates, send email, generate excel file in the database based on data obtained from BambooHR.

## Prerequisites
Before running the script, ensure you have the following prerequisites in place:

1. Python Environment: Make sure you have Python installed on your system.

2. Required Python Libraries: You will need to install the following Python libraries using `pip`:

   - `python-decouple`: For managing environment variables.
   - `requests`: For making API requests.
   - `ibm_db`: For connecting to the IBM Db2 database.
   - `Pandas`: For data managing.
   - `exchangelib`: For handling Outlook email operations.

3. Environment Variables: Set up the necessary environment variables for database, email and BambooHR API access. The script uses environment variables to securely store sensitive information like database credentials and API keys.

## Docker Install
**Clone Repository**
1. `git clone https://github.com/mamadoudiop-code/bamboohr-commission-schedule-rate-update.git`

**Moved into commission-schedule-rate-update.git DIR**

2. `$ cd commission-schedule-rate-update`

**Build Docker Container**

3. `docker build -t commission-schedule-rate-update .`

**Setting up the cronjob**

4. `$ crontab -e`

The script will run every Friday at 3 PM.

5. Add `0 15 * * 5  /usr/bin/docker run -d commission-schedule-rate-update`

To run the docker container manually you can run the following command:
`docker run commission-schedule-rate-update`

## Features

- **Data Processing**: Convert raw commission data into an Excel spreadsheet for easy manipulation and review.
- **Automated Email Notifications**: Send out bilingual (French and English) email notifications with the Excel file attached, informing about the update to the RPT_COMM_RATES_ELI table.
- **Configurable Recipients**: Email recipients are configurable, allowing for flexibility in notification distribution.

## Excel File Generation
The script creates an Excel file named `RPT_COMM_RATES_ELI Update.xlsx`, containing the following columns:

- Effective
- Type
- CLASS
- SITE_NAME
- USER_ID
- POOL
- RATE
- MULTIPLIER
- MULTIPLIER_EFFECTIVE
- END_DATE
- INS_TIMESTAMP
- status

## Email Notification

An HTML email body is created with bilingual instructions and warnings. It utilizes an `OutlookMessage` class to construct the email, including subject and recipients, and then attaches the Excel file if it exists.

The `pandas` DataFrame is used to structure the data, which is then written to an Excel file without the index column.

## Installation

1. Clone this repository or download the Python script to your local machine.

2. Install the required Python libraries using the following command:

   ```bash
   pip install python-decouple requests ibm_db pandas exchangelib

   or 

   pip install -r requirement.txt


## Configuration

Before running the script, you need to configure the following environment variables in a .env file in the same directory as the script:

- DRIVER: The IBM Db2 driver.
- DATABASE: The name of the database.
- HOSTNAME: The hostname or IP address of the database server.
- PORT: The port number for the database connection.
- PROTOCOL: The communication protocol (TCPIP).
- UID: The database username.
- PWD: The database password.
- authorization_key: The authorization key for BambooHR API access.
- SUBDOMAIN: The BambooHR subdomain for API requests.
- Email_secret: The password of integration team email
- Client_id : The ID provided by azure for sending email
- TENANT_ID: The tenant ID provided by azure for sending email authorization
- CLIENT_SECRET: The secret key provided by azure for sending Email
- Recipient: The recipient of the email 

## Usage

1. Configure the required environment variables in the .env file as described in the Configuration section.

2. Run the Python script using the following command:

```
python main.py

```
3. The script will connect to the IBM Db2 database, retrieve custom commission data, fetch employee data from BambooHR, compare the data, update the database accordingly, genereate an excel file and send email with the file attached.

