use <database>
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_data' AND xtype='U')
BEGIN
   drop table bloomberg_data
END
GO

CREATE TABLE bloomberg_data (
    request_id NVARCHAR(50) NOT NULL,
    identifier NVARCHAR(100) NOT NULL,
    request_name NVARCHAR(64) NOT NULL,
    data_type NVARCHAR(50) DEFAULT 'csv',
    data_content NTEXT,
    status NVARCHAR(12) DEFAULT 'pending' NOT NULL  CONSTRAINT bbg_data_status_check CHECK (status in ('pending', 'processing', 'completed')),
    ts DATETIME2 DEFAULT GETDATE(),
    PRIMARY key (request_id, data_type)
)
go

/* do we need an index on identifier */
CREATE NONCLUSTERED INDEX bbg_data_identifier_indx on dbo.bloomberg_data(identifier, ts)
go

CREATE NONCLUSTERED INDEX bbg_data_status_indx on dbo.bloomberg_data(status, ts)
go