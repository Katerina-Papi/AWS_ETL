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