import psycopg2 as psy
import boto3
import json
ssm_client = boto3.client('ssm')
# Get the SSM Param from AWS and turn it into JSON
# Don't log the password!
def get_ssm_param(param_name):
    print(f'get_ssm_param: getting param_name={param_name}')
    parameter_details = ssm_client.get_parameter(Name=param_name)
    redshift_details = json.loads(parameter_details['Parameter']['Value'])
    host = redshift_details['redshiftcluster-t8rsq42pcdhh.cqhp7bpa93jw.eu-west-1.redshift.amazonaws.com']
    user = redshift_details['brew_crew_user']
    db = redshift_details['brew_crew_cafe_db']
    print(f'get_ssm_param loaded for db={db}, user={user}, host={host}')
    return redshift_details
# Use the redshift details json to connect
def open_sql_database_connection_and_cursor(redshift_details):
    try:
        print('open_sql_database_connection_and_cursor: new connection starting...')
        db_connection = psy.connect(host=redshift_details['redshiftcluster-t8rsq42pcdhh.cqhp7bpa93jw.eu-west-1.redshift.amazonaws.com'],
                                    database=redshift_details['brew_crew_cafe_db'],
                                    user=redshift_details['brew_crew_user'],
                                    password=redshift_details['5COVTvO7eGDR0'],
                                    port=redshift_details['5439'])
        cursor = db_connection.cursor()
        print('open_sql_database_connection_and_cursor: connection ready')
        return db_connection, cursor
    except ConnectionError as ex:
        print(f'open_sql_database_connection_and_cursor: failed to open connection: {ex}')
        raise ex
    
COPY Order_Items (Order_Items_ID, Order_ID, Product_ID)
FROM 's3://brewcrew-clean-bucket/rawdata3.csv'
IAM_ROLE 'arn:aws:iam::339712886763:role/lambda-execution-role'
CSV
DELIMITER ','
IGNOREHEADER 1;

COPY Products (Product_ID, Product_Name, Product_Price)
FROM 's3://brewcrew-clean-bucket/rawdata3.csv'
IAM_ROLE 'arn:aws:iam::339712886763:role/lambda-execution-role'
CSV
DELIMITER ','
IGNOREHEADER 1;

COPY Orders (Order_ID, Location, Datetime, Total_Spent, Payment_Type)
FROM 's3://brewcrew-clean-bucket/rawdata3.csv'
IAM_ROLE 'arn:aws:iam::339712886763:role/lambda-execution-role'
CSV
DELIMITER ','
IGNOREHEADER 1;

COPY Location (Location, Date, Total_Sales)
FROM 's3://brewcrew-clean-bucket/rawdata3.csv'
IAM_ROLE 'arn:aws:iam::339712886763:role/lambda-execution-role'
CSV
DELIMITER ','
IGNOREHEADER 1;




export const handler = async (event) => {
  // TODO implement
  const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from Lambda!'),
  };
  return response;
};


