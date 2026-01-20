use master;
GO --sepaartor command in sql

/* check if database "Datawarehouse" already exists 
===================================================
if database exists, drop and recreate it.
create Database 'DataWarehouse' and schemas
=================================================== */
CREATE DATABASE Datawarehouse;
USE Datawarehouse;

--craeting schema - folder or container to keep things organised
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
