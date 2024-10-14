import pyodbc
from faker import Faker
import pandas as pd
import random
import csv

# Database connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-I0L9M47\Suleman;"  # Replace with your actual server name
    "Database=python;"               # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Initialize Faker
fake = Faker()
          #function to store catagories table by reading it from csv file


csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Categories.csv'


with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        # Join the rest of the row elements to form the description
        description = ','.join(row[2:])  # Get everything from the third index onward
        cursor.execute('''
            INSERT INTO Categories (CategoryID, CategoryName, Description)
            VALUES (?, ?, ?)
        ''', row[0], row[1], description)

#                            # inserting customer table data using its csv file
csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Customers.csv'

# # Load CSV data into a DataFrame
with open(csv_file_path, 'r', encoding='utf-8') as file:
        # Skip the header line
    next(file)
        
    for line in file:
            # Split the line by comma
        fields = line.strip().split(',')
        customer_id = fields[0]
        customer_name = fields[1]
        contact_name = fields[2]
        address = fields[3]
        city = fields[4]
        postal_code = fields[5]
        country = fields[6]
        try:
                # Insert into the Customers table
            cursor.execute('''
                INSERT INTO Customers (CustomerID, CompanyName, ContactName, Address, City, PostalCode, Country)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', customer_id, customer_name, contact_name, address, city, postal_code, country)
        except pyodbc.Error as e:
            print(f"Error inserting data for CustomerID {customer_id}: {e}")


                                #inserting in order details table
csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Order_details.csv'


with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        
        for row in reader:
            order_detail_id = int(row[0])
            order_id = int(row[1])
            product_id = int(row[2])
            quantity = int(row[3])
            
            # Insert data into the table (skipping OrderDetailID, as it's an IDENTITY column)
            cursor.execute('''
                INSERT INTO OrderDetails (OrderID, ProductID, Quantity)
                VALUES (?, ?, ?)
            ''', order_id, product_id, quantity)


                            # inserting data in order table
csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Orders.csv'

with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
        cursor.execute('''
            INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipVia)
            VALUES (?, ?, ?, ?, ?)
        ''', row[0], row[1].strip(), row[2], row[3], row[4])

                              # inserting data of shippers table
csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Shippers.csv'

with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        cursor.execute('''
            INSERT INTO Shippers (ShipperID, CompanyName, Phone)
            VALUES (?, ?, ?)
        ''', row[0], row[1], row[2])

                    #inserting data in table Products

csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Products.csv'


with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        # Strip whitespace from each cell in the row
        row = [cell.strip() for cell in row]

        # Check if the row has the expected number of columns (6 in this case)
        if len(row) == 6:
            cursor.execute('''
                INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', row[0], row[1], row[2], row[3], row[4], row[5])
        else:
            print(f"Skipping row due to unexpected number of columns: {row}")
                            # insertig data in suppliers
csv_file_path = r'DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Suppliers.csv'


with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        cursor.execute('''
            INSERT INTO Suppliers (SupplierID, CompanyName, ContactName, Address, City, PostalCode, Country, Phone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])


conn.commit()
    # print("All tables created successfully.")

# except pyodbc.Error as e:
#     print("Error creating tables:", e)

# finally:
cursor.close()
conn.close()