import csv

fieldnames = ['customer_id', 'purchase_date', 'purchase_amount', 'product_id'] # assigning column names 
cleaned_data = [] # creating empty list to put clean data in
with open ('sales_data.csv', 'r') as datafile:
    sales_csv = csv.DictReader(datafile, fieldnames=fieldnames, delimiter=',') # reading sales data csv file
    
    for row in sales_csv: # iterates through each row in csv file
        if all(row.values()): # 'all' checks to see if they are empty values, then checks the below
            if row[fieldnames[1]] >= '2020-12-01' and row[fieldnames[1]] <= '2020-12-05': # checks which values fit in these dates
                cleaned_data.append(row) # adds the above 'cleaned' data to the empty list
            
for row in cleaned_data:
    print("Customer ID:", row['customer_id'])
    print("Purchase Date:", row['purchase_date'])
    print("Purchase Amount:", row['purchase_amount'])
    print("Product ID:", row['product_id'], "\n")
    
    
# Total spent and Average spent of each customer ID
customer_totals = {} # empty dictionary for cutsomer totals
purchase_count = {} # empty dictionary for amount of purchases each customer has made
average_spent = {} # empty dictionary to hold average spend

for row in cleaned_data: # iterates through each row in the clean data
    customer_id = row['customer_id'] # finding customer ids
    purchase_amount = float(row['purchase_amount']) # finding purchase amounts
    
    if customer_id in customer_totals: # if the customer id is already in the totals dict...
        customer_totals[customer_id] += purchase_amount # add that IDs purchase amount to customer totals
        purchase_count[customer_id] += 1 # add 1 to purchase count to show that's another purchase they've made
    else: # if the customer id is not already in totals dictionary...
        customer_totals[customer_id] = purchase_amount # assigns next purchase amount to appropriate customer id
        purchase_count[customer_id] = 1 # assign to one to then start adding to if make more purchases
        
for customer, spend in customer_totals.items(): # formats customer id and total spend 
    print(f"Customer {customer} has spent a total of: £{spend:.2f}") # :.2f will show float to 2 decimal places

# iterates through customer IDs in customer totals
# then calculates average spent for each cust. and assigns to average spent    
for customer_id in customer_totals:
    average_spent[customer_id] = customer_totals[customer_id] / purchase_count[customer_id]
    
for customer, av_spend in average_spent.items(): # formats customer and average spent
    print(f"Customer ID {customer} has spent an average of {av_spend:.2f}") # :.2f will show float to 2 decimal places


with open ('clean_sales_data.csv', 'w') as clean_datafile:
          clean_cvs = csv.DictWriter(clean_datafile, fieldnames=fieldnames, delimiter=',') 
          clean_cvs.writeheader()
          clean_cvs.writerows(cleaned_data)