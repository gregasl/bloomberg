use playdb
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_data_def' AND xtype='U')
BEGIN
   drop table bloomberg_data_def
END
GO

CREATE TABLE bloomberg_data_def (
    request_name NVARCHAR(12) NOT NULL PRIMARY KEY,
    request_col_name NVARCHAR(128) NOT NULL, /* many times these 2 will be the same */
    reply_col_name NVARCHAR(128) NOT NULL,
    data_type NVARCHAR(12) NOT NULL DEFAULT 'CSV', /* lets use this later */
    output_col_name NVARCHAR(128) NULL,
    output_file_name NVARCHAR(128) NULL,
    db_col_Name NVARCHAR(128) NOT NULL,
    ts DATETIME2 NOT NULL DEFAULT (GETDATE())
)
go

/* not sure we need any indexes unless table gets huge. */
CREATE NONCLUSTERED INDEX col_request_index on dbo.bloomberg_data_def(request_col_name, request_name)
go