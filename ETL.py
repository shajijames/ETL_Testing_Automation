# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 18:34:43 2019

@author: SSelvaku
"""

import pyodbc
import pandas as pd

mssql_con=pyodbc.connect(r'DSN=MSSQL_DSN;'
                'Authentication=ActiveDirectoryPassword')

mysql_con=pyodbc.connect(r'DSN=MySQL_DSN')

fact_invoice="select distinct b.ShipName, b.ShipAddress, b.ShipCity, b.ShipRegion, b.ShipPostalCode, b.ShipCountry, b.CustomerID, c.CompanyName, c.Address, c.City, c.Region, c.PostalCode, c.Country, concat(d.FirstName, ' ', d.LastName) as Salesperson, b.OrderID, b.OrderDate, b.RequiredDate, b.ShippedDate, e.ProductID, f.ProductName, e.UnitPrice, e.Quantity, e.Discount, e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice, b.Freight from Northwind.dbo.Shippers a inner join Northwind.dbo.Orders b on a.ShipperID = b.ShipVia inner join Northwind.dbo.Customers c on c.CustomerID = b.CustomerID inner join Northwind.dbo.Employees d on d.EmployeeID = b.EmployeeID inner join Northwind.dbo.Order_Details e on b.OrderID = e.OrderID inner join Northwind.dbo.Products f on f.ProductID = e.ProductID;"

fact_order_details_extended="select distinct y.OrderID, y.ProductID, x.ProductName, y.UnitPrice, y.Quantity, y.Discount, round(y.UnitPrice * y.Quantity * (1 - y.Discount), 2) as ExtendedPrice from Northwind.dbo.Products x inner join Northwind.dbo.Order_Details y on x.ProductID = y.ProductID;"

dim_products="select distinct b.*, a.CategoryName from Northwind.dbo.Categories a inner join Northwind.dbo.Products b on a.CategoryID = b.CategoryID;"

dim_demography="select a.TerritoryID,a.TerritoryDescription,a.RegionID,b.RegionDescription from Northwind.dbo.Territories a left join Northwind.dbo.Region b on a.RegionID=b.RegionID;"

df_fact_invoice=pd.read_sql(fact_invoice,mssql_con)

df_fact_order_details_extended=pd.read_sql(fact_order_details_extended,mssql_con)

df_dim_products=pd.read_sql(dim_products,mssql_con)

df_dim_demography=pd.read_sql(dim_demography,mssql_con)


import pymysql
import sqlalchemy
conn=sqlalchemy.create_engine("mysql+pymysql://demo:mysqldemo@localhost/demo?charset=utf8mb4")

df_fact_invoice.to_sql('fact_invoice',conn,if_exists='replace',index=False)

df_fact_order_details_extended.to_sql('fact_order_details_extended',conn,if_exists='replace',index=False)

df_dim_products.to_sql('dim_products',conn,if_exists='replace',index=False)

df_dim_demography.to_sql('dim_demography',conn,index=False)

pd.read_sql("select * from dim_demography;",conn)

