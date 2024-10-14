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

create_table_ship = '''CREATE TABLE Shippers (
    ShipperID INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    Phone VARCHAR(24)
);'''

create_table_cust ='''CREATE TABLE Customers (
    CustomerID VARCHAR(50) PRIMARY KEY,
    CompanyName VARCHAR(255),
    ContactName VARCHAR(30),
    ContactTitle VARCHAR(30),
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    Phone VARCHAR(24),
    Fax VARCHAR(24)
);'''

create_table_supp ='''CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    ContactName VARCHAR(30),
    ContactTitle VARCHAR(30),
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Homepage TEXT
);'''

create_table_categ ='''CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(255),
    Description TEXT,
    Picture IMAGE
);'''

create_table_prod ='''CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit VARCHAR(255),
    UnitPrice DECIMAL(10, 2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT,
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);'''

create_table_order='''CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID VARCHAR(50),
    EmployeeID INT,
    OrderDate DATE,
    RequiredDate DATE,
    ShippedDate DATE,
    ShipVia INT,
    Freight DECIMAL(10, 2),
    ShipName VARCHAR(255),
    ShipAddress VARCHAR(255),
    ShipCity VARCHAR(50),
    ShipRegion VARCHAR(50),
    ShipPostalCode VARCHAR(20),
    ShipCountry VARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ShipVia) REFERENCES Shippers(ShipperID)
);'''

create_table_order_det ='''CREATE TABLE OrderDetails (
    OrderID INT,
    ProductID INT,
    UnitPrice DECIMAL(10, 2),
    Quantity SMALLINT,
    Discount DECIMAL(5, 2),
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);'''
#altering order detail id as it has not orderdetailID in it
# cursor.execute('''
#         ALTER TABLE OrderDetails
#         ADD OrderDetailID INT IDENTITY(1,1);
#     ''')

create_table_emp ='''CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    LastName VARCHAR(255),
    FirstName VARCHAR(255),
    Title VARCHAR(50),
    TitleOfCourtesy VARCHAR(25),
    BirthDate DATE,
    HireDate DATE,
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    HomePhone VARCHAR(24),
    Extension VARCHAR(4),
    Photo IMAGE,
    Notes TEXT,
    ReportsTo INT,
    PhotoPath VARCHAR(255),
    FOREIGN KEY (ReportsTo) REFERENCES Employees(EmployeeID)
);'''

create_table_reg ='''CREATE TABLE Regions (
    RegionID INT PRIMARY KEY,
    RegionDescription VARCHAR(50)
);'''

create_table_terr ='''CREATE TABLE Territories (
    TerritoryID VARCHAR(20) PRIMARY KEY,
    TerritoryDescription VARCHAR(50),
    RegionID INT,
    FOREIGN KEY (RegionID) REFERENCES Regions(RegionID)
);'''

create_table_empterr ='''CREATE TABLE EmployeeTerritories (
    EmployeeID INT,
    TerritoryID VARCHAR(20),
    PRIMARY KEY (EmployeeID, TerritoryID),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (TerritoryID) REFERENCES Territories(TerritoryID)
);'''
           
def populate_regions(n):
    for _ in range(n):
        region_id = random.randint(1, 1000)  # Generate random region_id
        region_desc = fake.city_suffix()     # Fake city suffix for region description
        cursor.execute(
            "INSERT INTO Regions (RegionID, RegionDescription) VALUES (?, ?)",
            region_id, region_desc
        )

# Function to populate Territories
def populate_territories(n):
    cursor.execute("SELECT RegionID FROM Regions")  # Get available RegionIDs
    region_ids = [row[0] for row in cursor.fetchall()]

    for _ in range(n):
        territory_id = fake.unique.zipcode()          # Fake and unique territory id
        territory_desc = fake.city()                 # Fake city name for description
        region_id = random.choice(region_ids)        # Randomly pick a RegionID
        cursor.execute(
            "INSERT INTO Territories (TerritoryID, TerritoryDescription, RegionID) VALUES (?, ?, ?)",
            territory_id, territory_desc, region_id
        )

# Populate the tables
#populate_regions(10)  # Populate 10 regions
#populate_territories(20)  # Populate 20 territories
# try:
#     cursor.execute(create_table_ship)
#     cursor.execute(create_table_cust)
#     cursor.execute(create_table_supp)
#     cursor.execute(create_table_categ)
#     cursor.execute(create_table_prod)
#     cursor.execute(create_table_order)
#     cursor.execute(create_table_order_det)
#     cursor.execute(create_table_emp)
#     cursor.execute(create_table_reg)
#     cursor.execute(create_table_terr)
#     cursor.execute(create_table_empterr)

    # Commit the changes
conn.commit()
    # print("All tables created successfully.")

# except pyodbc.Error as e:
#     print("Error creating tables:", e)

# finally:
cursor.close()
conn.close()