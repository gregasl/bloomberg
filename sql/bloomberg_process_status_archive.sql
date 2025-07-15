use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_process_status_archive' AND xtype='U')
begin
    drop table bloomberg_process_status_archive
end
go

/*  Create requests table */
CREATE TABLE bloomberg_process_status_archive (
    id INT IDENTITY PRIMARY key, 
    request_id NVARCHAR(50) NOT NULL,
    identifier NVARCHAR(100) NOT NULL,
    name NVARCHAR(64) NOT NULL,
    process_type NVARCHAR(64) NOT NULL, 
    processed_status NVARCHAR(20) NOT NULL,
    process_error NVARCHAR(256) NULL,
    ts DATETIME2 NOT NULL,
    archive_type CHAR DEFAULT 'I'  CONSTRAINT proc_stat_arch_type_check CHECK (archive_type in ('I', 'U', 'D')),
)
go

CREATE NONCLUSTERED INDEX bloomberg_process_status_archive_request_id_indx on dbo.bloomberg_process_status_archive(request_id, id)
GO




