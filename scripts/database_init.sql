
USE master;
GO

CREATE DATABASE dwh_db;
GO

USE dwh_db;
GO

CREATE SCHEMA bronze;
GO

SELECT name
FROM sys.schemas
WHERE name = 'bronze';
