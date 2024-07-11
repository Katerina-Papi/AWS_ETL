import pandas as pd
data = pd.read_csv('chesterfield_25-08-2021_09-00-00_(3).csv')
data.columns = ['datetime','loc','name','orderandcost','total','paytype','cardno']
#print(data)
print(data.head(20)) #prints first 20 rows of data
#print(data.tail(20)) #prints last 20 rows of data
print(data.info()) #info about data
#data['cardno'].fillna('no_val', inplace=True ) #substitutes empty value with no_val for cardno
#data.fillna('no_val', inplace=True ) #substitutes empty value with no_val
#print(data.duplicated()) #check for duplicates
data[['date', 'time']] = data['datetime'].str.split(' ', n=1, expand = True)# parses date and time expands into new columns
#data.drop(columns='datetime') #doesn't drop column as supposed to...
print(data)
print(data.info()) # information about imported data
#print(data.head(20))
#print(data.iloc[:, [8,7,1,2,3,4,5]]) #rearranges the column as in the brackets, excludes datetime and card number
#print(data.iloc[:20, [8,7,1,2,3,4,5]]) #rearranges the column as in the brackets, excludes datetime and card number and 1st 20 rows

data2 = data.iloc[:, [8,7,1,2,3,4,5]] 
#counts = data['name'].value_counts() #displays the occurence frequency in list of names
counts = data.groupby('name').size() # displays the occurence frequency of names in list
print(counts.head(20))

print(data2)
print(data2.loc[1])
b = data2.loc[1]
c = b.loc['orderandcost']
d = c.split(',') #makes list of products and costs for order
print(d)

e = d[0][::-1].split('-',1) #turns products and cost into list read string backwards, only performs seperation once
e = e[::-1] #reverses list order
e[0] = e[0][::-1] #reverses item one in list
e[1] = e[1][::-1] #reverses item two in list, now order unreversed but with the but with the list correctly split before the cost

#print(e) #end process, next turn the first item in list into a key and convert the next into a float(value)
f = e[1] #the cost of item
e[1] = float(f) #turn from string to float
#print(e)
order_item = {e[0]:e[1]} #convert to dictionary, key is product, value is cost
print(order_item)
a1 = data2.count(1)
print(len(data2))

for i in range(len(data2)):
    b = data2.loc[i]
    c = b.loc['orderandcost']
    d = c.split(',')
    for i in range(len(d)):
        e = d[i][::-1].split('-',1) #turns products and cost into list read string backwards, only performs seperation once
        e = e[::-1] #reverses list order
        e[0] = e[0][::-1] #reverses item one in list
        e[1] = e[1][::-1] #reverses item two in list, now order unreversed but with the but with the list correctly split before the cost
        f = e[1] #the cost of item
        e[1] = float(f) #turn from string to float
        order_item = {e[0]:e[1]} #convert to dictionary, key is product, value is cost
        d[i] = order_item
    b.loc['orderandcost'] = d
        
        
    










