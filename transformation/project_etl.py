import csv

with open ('chesterfield_25-08-2021_09-00-00_(3).csv', 'r') as file:
    fieldnames = ['date & time', 'location', 'customer_name', 'products_and_costs', 'total_spend', 'payment_type', 'card_number'] # assigning column names
    order_csv = csv.DictReader(file,fieldnames = fieldnames)
#sales_list is list of dictionaries 
    order_list = [row for row in order_csv]
#print(order_list)
a = order_list[267] 
print(a['payment_type'])
#print(a['customer_name'])
name_in = []
name_out = []
order_out = order_list

for i,num in enumerate(order_list): #this block converts names into unique dictionary strings
    name_in.append(num['customer_name'])
name_in = dict.fromkeys(name_in)

for i,a in enumerate(name_in): #creates substitute values for customer names
    cust_alias = 'cust' + str(i)
    name_in[a] = cust_alias

for i,item in enumerate(order_list): #substitues values inserted into order_out
    i = int(i)
    d_entry = order_out[i]
    val1 = d_entry['customer_name']
    d_entry['customer_name'] = name_in[val1]
    order_out[i] = d_entry
#print(order_out)
print(order_list[1]) #start process of extracting product and cost for key-value pairs
b = order_list[1] 
c = b['products_and_costs']
d = c.split(',') #makes list of products and costs for order
print(d)

e = d[0][::-1].split('-',1) #turns products and cost into list read string backwards, only performs seperation once
e = e[::-1] #reverses list order
e[0] = e[0][::-1] #reverses item one in list
e[1] = e[1][::-1] #reverses item two in list, now order unreversed but with the but with the list correctly split before the cost

print(e) #end process, next turn the first item in list into a key and convert the next into a float(value)
f = e[1] #the cost of item
e[1] = float(f) #turn from string to float
print(e)
order_item = {e[0]:e[1]} #convert to dictionary, key is product, value is cost
print(order_item)
    


            
