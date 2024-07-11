#Connect to the cluster and create a Cursor
import redshift_connector
with redshift_connector.connect(...) as conn:
with conn.cursor() as cursor:

#Use COPY to copy the contents of the S3 bucket into the empty table 
cursor.execute("copy category from 's3://testing/category_csv.txt' iam_role 'arn:aws:iam::123:role/RedshiftCopyUnload' csv;")

#Retrieve the contents of the table
cursor.execute("select * from category")
print(cursor.fetchall())

