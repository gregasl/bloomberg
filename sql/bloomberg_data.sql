use playdb
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_data' AND xtype='U')
BEGIN
   drop table bloomberg_data
END
GO

CREATE TABLE bloomberg_data (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    request_id NVARCHAR(50) NOT NULL,
    identifier NVARCHAR(100) NOT NULL,
    data_type NVARCHAR(50) DEFAULT 'csv',
    raw_data NTEXT,
    parsed_data NTEXT,
    snapshot_timestamp NVARCHAR(50),
    record_count INT,
    created_at DATETIME2 DEFAULT GETDATE()
)
go

/* do we need an index on identifier */
CREATE NONCLUSTERED INDEX request_id_indx on dbo.bloomberg_data(request_id)
go