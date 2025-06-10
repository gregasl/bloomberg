use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_requests_trig' AND xtype='TR')
begin
    drop trigger bloomberg_requests_trig
end
go

/*  Create requests table */
create trigger bloomberg_requests_trig on  bloomberg_requests
    AFTER UPDATE
    NOT FOR REPLICATION
    as 
    BEGIN
        IF exists(select * from inserted)
        BEGIN
            UPDATE br
                SET ts = GETDATE() 
            FROM dbo.bloomberg_requests AS br
            INNER JOIN Inserted AS i
                ON br.request_id = i.request_id;
        END
    END
go

