

import json
import boto3
import pandas as pd
import csv
from io import StringIO
import psycopg2 as psy

s3 = boto3.client('s3')
session = boto3.Session()
ssm_client = session.client('ssm')

def get_ssm_param(param_name):
    print(f'get_ssm_param: getting param_name={param_name}')
    parameter_details = ssm_client.get_parameter(Name=param_name)
    redshift_details = json.loads(parameter_details['Parameter']['Value'])

    host = redshift_details['host']
    user = redshift_details['user']
    db = redshift_details['database-name']
    print(f'get_ssm_param loaded for db={db}, user={user}, host={host}')
    return redshift_details
    
def process_products(response):
    # Read the CSV data from the response object
    csv_data = response['Body'].read().decode('utf-8')
    
    # Initialize an empty list to store the dictionaries
    list_of_dicts = []
    
    # Parse the CSV data and convert it into dictionaries
    csv_reader = csv.DictReader(StringIO(csv_data))
    for row in csv_reader:
        # Convert keys to lowercase for consistency
        row_lower = {key.lower(): value for key, value in row.items()}
        list_of_dicts.append(row_lower)
    
    return list_of_dicts

def lambda_handler(event, context):
    try:
        ssm_param_name = get_ssm_param('brew_crew_redshift_settings')
        print(ssm_param_name)

        conn = psy.connect(
            database=ssm_param_name['database-name'],
            host=ssm_param_name['host'],
            port=ssm_param_name['port'],
            password=ssm_param_name['password'],
            user=ssm_param_name['user']
        )   

        curr = conn.cursor()
        
        print("connected")
    
        source_bucket_name = 'brewcrew-clean-bucket'
        print("event", event)
        file_key = event['Records'][0]['s3']['object']['key']
        
        print("file key", file_key)
            
        # Read the CSV file from S3
        response = s3.get_object(Bucket=source_bucket_name, Key=file_key)
    
        print("response ", response)
        
        # Process CSV data
        list_of_dicts = process_products(response)
        print(len(list_of_dicts))
        print(type(list_of_dicts))
        print(list_of_dicts)
        
    
        # Insert records into the Redshift table if they don't already exist
        # PRODUCTS LOADING 
        
        for record in list_of_dicts:
            curr.execute(
                "SELECT COUNT(*) FROM products WHERE product_name = %s AND product_price = %s",
                (record['product_name'], record['product_price'])
            )
            count = curr.fetchone()[0]
            if count == 0:
                curr.execute(
                    "INSERT INTO products (product_name, product_price) VALUES (%s, %s)",
                    (record['product_name'], record['product_price'])
                )
                print("producs loaded")
                
        # for record in list_of_dicts:
        #     curr.execute(
        #         "SELECT COUNT(*) FROM location WHERE location = %s AND date = %s AND total_sales = %s",
        #         (record['location'], record['date'], record['total_sales'])
        #     )
        #     count = curr.fetchone()[0]
        #     if count == 0:
        #         curr.execute(
        #             "INSERT INTO location (location, date, total_sales)
        #             SELECT o.location, o.date, SUM(o.total) as total_sales
        #             FROM orders o
        #             GROUP BY o.location, o.date;"
                
        # ORDERS LOADING
        
        for record in list_of_dicts:
            curr.execute(
                "SELECT COUNT(*) FROM orders WHERE order_id = %s AND location = %s AND date = %s AND time = %s AND total = %s AND payment_type = %s",
                (record['order_id'], record['location'], record['date'], record['time'], record['total'], record['payment_method'])
            )
            count = curr.fetchone()[0]
            if count == 0:
                curr.execute(
                    "INSERT INTO orders (order_id, location, date, time, total, payment_type) VALUES (%s, %s, %s, %s, %s, %s)",
                    (record['order_id'], record['location'], record['date'], record['time'], record['total'], record['payment_method'])
                )     
                print("orders loaded")
         
        # ORDER ITEMS LOADING
        
        for row in list_of_dicts:
            try:
                curr.execute("""
                    SELECT product_id
                    FROM products
                    WHERE product_name = %s AND product_price = %s
                """, (row['product_name'], row['product_price']))

                product_result = curr.fetchone()

                if product_result:
                    product_id = product_result[0]
                    curr.execute("""
                        SELECT order_id
                        FROM  orders
                        WHERE order_id = %s AND location = %s AND date = %s AND time = %s AND total = %s AND payment_type = %s
                    """, (row['order_id'], row['location'], row['date'], row['time'], row['total'], row['payment_method']))

                    order_result = curr.fetchone()

                    if order_result:
                        order_id = order_result[0]
                        curr.execute("""
                            INSERT INTO order_items (order_id, product_id)
                            VALUES (%s, %s)
                        """, (order_id, product_id))
                        print("order items added succesfully")
                    else:
                        print("No matching order found for row:", row)
                else:
                    print("No matching product found for row:", row)
            except Exception as e:
                print(f"Error inserting record: {e}")
        
       
        # for record in list_of_dicts: 
        #     curr.execute(
        #         "SELECT COUNT(*) FROM order_items WHERE order_items_id = %s AND order_id = %s AND product_id = %s",
        #         (record['order_items_id'], record['order_id'], record['product_id'])
        #     )
        #     count = curr.fetchone()[0]
        #     if count == 0:
        #         curr.execute(
        #             "INSERT INTO order_items (order_items_id, order_id, product_id) VALUES (%s, %s, %s)",
        #             (record['order_items_id'], record['order_id'], record['product_id'])
        #         )           
        
        print("all records inserted")
        conn.commit()

        curr.close()
        conn.close() 

        return {
            'statusCode': 200,
            'body': 'Processed uploaded CSV files successfully'
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': 'Error processing uploaded CSV files'
        }
