/*
- The script uses to initialize the database including create database (check if exists then drop and recreate) and schemas 
*/

-- Use 'master' database
use master;
go

-- Drop and recreate 'DataWarehouse' database
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go

create database DataWarehouse;
go

use DataWarehouse;
go

-- Create schemas
create schema Bronze;
go
create schema Silver;
go
create schema Gold;
go