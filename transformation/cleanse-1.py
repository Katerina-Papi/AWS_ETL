import pandas as pd

headers = ['Datetime','Location','Customer_name','Order_items','Amount','Payment_type','Card_number']

df = pd.read_csv('sales-data.csv', header=None, names=headers)
df.drop(columns=['Customer_name','Card_number'], inplace=True)
df.to_csv('pd_index_test.csv',index = True, index_label='order_id')
print(df)