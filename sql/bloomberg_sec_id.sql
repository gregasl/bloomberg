use playdb
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_sec_id' AND xtype='U')
BEGIN
   drop table bloomberg_sec_id
END
GO

CREATE TABLE bloomberg_sec_id (
   sec_id_type NVARCHAR(8) NOT NULL CONSTRAINT bbg_sec_id_sec_id_type_check CHECK (sec_id_type in ('FUT', 'MBS', 'TSY')),
   sec_id NVARCHAR(32) NOT NULL,
   ts DATETIME2 DEFAULT GETDATE(),
   PRIMARY KEY(sec_id, sec_id_type)
)
go
