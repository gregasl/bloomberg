/* not sure what this is for */
/* IDK we need this */

use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_processed_responses' AND xtype='U')
BEGIN
   drop table bloomberg_processed_responses
END
GO

CREATE TABLE bloomberg_processed_responses (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    request_id NVARCHAR(50) NOT NULL,
    response_id NVARCHAR(50),
    handler_name NVARCHAR(100),
    processing_status NVARCHAR(20) DEFAULT 'success',
    processing_message NTEXT,
    processed_at DATETIME2 DEFAULT GETDATE(),
    processing_duration_ms INT
)

go

CREATE NONCLUSTERED INDEX request_id_indx on dbo.bloomberg_processed_responses(request_id)
go