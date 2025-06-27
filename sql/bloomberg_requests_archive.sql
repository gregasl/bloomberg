use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_requests_archive' AND xtype='U')
begin
    drop table bloomberg_requests_archive
end
go

/*  Create requests archive table */
CREATE TABLE bloomberg_requests_archive (
    id INT IDENTITY PRIMARY key, 
    request_id NVARCHAR(50),
    identifier NVARCHAR(100) NOT NULL,
    name NVARCHAR(64) NOT NULL,
    title NVARCHAR(64) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    priority INT NOT NULL,
    response_id  NVARCHAR(50) NULL,
    response NVARCHAR(128) NULL,
    response_status NVARCHAR(20),
    request_retry_count INT NOT NULL,
    max_request_retries INT NOT NULL,
    retry_wait_sec INT NOT NULL,
    response_poll_count INT NOT NULL,
    max_response_polls INT NOT NULL,
    response_poll_wait_sec INT NOT NULL,
    last_poll_at DATETIME2 NULL,
    created_at DATETIME2 NOT NULL,
    submitted_at DATETIME2 NULL,
    completed_at DATETIME2  NULL,
    updated_at DATETIME2 NULL,
    ts DATETIME2 DEFAULT GETDATE(),
    archive_type CHAR DEFAULT 'I'  CONSTRAINT arch_type_check CHECK (archive_type in ('I', 'U', 'D')),
)
go

CREATE NONCLUSTERED INDEX bloomberg_requests_archive_request_id_indx on dbo.bloomberg_requests_archive(request_id, id)
GO

