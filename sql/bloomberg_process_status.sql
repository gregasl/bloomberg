use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_process_status' AND xtype='U')
begin
    drop table bloomberg_process_status
end
go

/*  Create requests table */
CREATE TABLE bloomberg_process_status (
    request_id NVARCHAR(50),
    identifier NVARCHAR(100) NOT NULL,
    name NVARCHAR(64) NOT NULL,
    process_type NVARCHAR(16) NOT NULL,
    processed_status NVARCHAR(20) DEFAULT 'pending' NOT NULL  CONSTRAINT bbg_processed_status_check CHECK (processed_status in ('pending', 'error', 'processed')),
    process_error NVARCHAR(256) NULL,
    ts DATETIME2 DEFAULT GETDATE()
    PRIMARY key(request_id, process_type)
)
go

ALTER TABLE bloomberg_process_status ADD CONSTRAINT FK_process_type_constraint 
    FOREIGN key (process_type)
    REFERENCES bloomberg_output_types(output_type) 
    ON DELETE CASCADE
    on UPDATE CASCADE
go

