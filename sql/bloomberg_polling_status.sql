/* not sure what this is for */
/* IDK we need this */

use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_polling_status' AND xtype='U')
BEGIN
   drop table bloomberg_polling_status
END
GO

CREATE TABLE bloomberg_polling_status (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    request_id NVARCHAR(50) NOT NULL,
    identifier NVARCHAR(100) NOT NULL,
    poll_count INT DEFAULT 0,
    max_polls INT DEFAULT 240,
    last_poll_at DATETIME2 DEFAULT GETDATE(),
    status NVARCHAR(20) DEFAULT 'polling',
    created_at DATETIME2 DEFAULT GETDATE()
)
go

CREATE NONCLUSTERED INDEX request_id_indx on dbo.bloomberg_polling_status(request_id)
go