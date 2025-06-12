use playdb
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_data_def' AND xtype='U')
BEGIN
   drop table bloomberg_data_def
END
GO

CREATE TABLE bloomberg_data_def (
    request_name NVARCHAR(24) NOT NULL,
    is_variable_data TINYINT not null DEFAULT 1,
    suppress_sending TINYINT not null DEFAULT 0,  /* for testing send only problematic ones -- */
                                                 /* or turn off if no longer needed */
    request_col_name NVARCHAR(128) NOT NULL, /* many times these 2 will be the same */
    reply_col_name NVARCHAR(128) NOT NULL,
    data_type NVARCHAR(12) NOT NULL DEFAULT 'TEXT', /* lets use this later */
    output_col_name NVARCHAR(128) NULL,
    db_col_name NVARCHAR(128) NOT NULL,
    ts DATETIME2 NOT NULL DEFAULT (GETDATE())
    PRIMARY KEY(request_col_name, request_name)
)
go
