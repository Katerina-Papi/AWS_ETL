import csv

fieldnames = ['date & time', 'location', 'customer_id', 'product', 'total_spend', 'payment_type'] # assigning column names 
cleaned_data = [] # creating empty list to put clean data in
with open ('chesterfield_25-08-2021_09-00-00_(3).csv', 'r') as datafile:
    sales_csv = csv.DictReader(datafile, fieldnames=fieldnames, delimiter=',') # reading sales data csv file
        
    for row in sales_csv:
        # if there is any info after 'payment_type' value, then remove 
        # or if there is an int longer than 3 digits, remove
        if 