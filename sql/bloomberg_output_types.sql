use <database>
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_output_types' AND xtype='U')
BEGIN
   drop table bloomberg_output_types
END
GO

CREATE TABLE bloomberg_output_types (
    output_type NVARCHAR(16) PRIMARY KEY
)
go

insert into bloomberg_output_types VALUES ('database'),('csv'),('raw')
go

