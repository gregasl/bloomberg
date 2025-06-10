use playdb
GO

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_data_def' AND xtype='U')
BEGIN
   drop table bloomberg_data_def
END
GO

CREATE TABLE bloomberg_data_def (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    request_col_name NVARCHAR(128) NOT NULL, /* many times these 2 will be the same */
    reply_col_name NVARCHAR(128) NOT NULL,
    request_type NVARCHAR(12) DEFAULT 'GOVT_INST' NOT NULL  CONSTRAINT bbg_data_def_request_type_check CHECK (request_type in ('GOVT_INST', 'MBS_INST', 'FUT_INST')),
    data_type NVARCHAR(12) DEFAULT 'TEXT', /* lets use this later */
    speadsheet_col_name VARCHAR(128) NOT NULL,
    db_col_Name VARCHAR(128) NOT NULL,
)
go

/* not sure we need any indexes unless table gets huge. */
CREATE NONCLUSTERED INDEX col_request_index on dbo.bloomberg_data_def(bbg_request_col_name, request_type)
go