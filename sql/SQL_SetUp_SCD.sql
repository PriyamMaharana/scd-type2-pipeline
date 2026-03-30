-- creating database
IF DB_ID('SCD_Project') IS NOT NULL
	BEGIN
		PRINT 'The database already exists.';
	END
ELSE 
	BEGIN 
		PRINT 'The database does not exists.. pls wait creating one!!'
		CREATE DATABASE SCD_Project;
		PRINT 'Database Created.!!'
	END
GO

-- using database
IF DB_ID('SCD_Project') IS NOT NULL
	BEGIN 
		USE SCD_Project;
		PRINT 'SUCCESS: The database SCD_Project is been used.'
	END
ELSE
	BEGIN 
		PRINT 'ALERT: The database does not exist!! \nPlease ensure that database exsits.';
	END
GO


-- creating the SCD Type 2 Dimension Table
CREATE TABLE dim_customer (
	customer_sk INT PRIMARY KEY IDENTITY(1,1),
	customer_id INT NOT NULL,
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	email NVARCHAR(100),
	city NVARCHAR(50),
	state NVARCHAR(50),
	customer_segment NVARCHAR(20),
	effective_date DATE NOT NULL,
	expiry_date DATE NOT NULL,
	is_current BIT NOT NULL DEFAULT 1,
	created_at DATETIME2 DEFAULT GETDATE(),
	updated_at DATETIME2 DEFAULT GETDATE()
);
GO


-- creating staging table (raw data land here)
CREATE TABLE stg_customer (
	customer_id INT,
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	email NVARCHAR(100),
	city NVARCHAR(50),
	state NVARCHAR(50),
	customer_segment NVARCHAR(20),
	source_date DATE
);
GO


-- create audit/log table (track every SCD operation that runs)
CREATE TABLE scd_audit_log (
	lod_id  INT PRIMARY KEY IDENTITY(1,1),
	run_date DATETIME2 DEFAULT GETDATE(),
	customer_id INT,
	operation NVARCHAR(30),
	changed_columns	NVARCHAR(200),
	old_value NVARCHAR(500),
	new_value NVARCHAR(500)
);
GO


-- creating indexes for performance
CREATE INDEX ix_dim_customer_id
ON dim_customer(customer_id);

CREATE INDEX ix_dim_customer_current
ON dim_customer(customer_id, is_current);

GO

PRINT 'Database Setup Completed.!!'

GO 
select * from stg_customer;
select count(*) from dim_customer;
select * from scd_audit_log;

truncate table stg_customer;
truncate table dim_customer;
truncate table scd_audit_log;

-- performing operations
update dim_customer
set expiry_date = '2024-02-28', is_current = 0
where customer_id = 10 and is_current = 1;

insert into dim_customer (
	customer_id, first_name, last_name, email, city, state, customer_segment,
	effective_date, expiry_date, is_current, created_at, updated_at
) values (
	10, 'Jack', 'Chaudhari', 'customer10@gmail.com', 'Pune', 'Maharashtra', 'Regular', CAST(GETDATE() as Date), '9999-12-31',
	1, GETDATE(), GETDATE()
);

create procedure sp_insert_customer_scd2
	@p_customer_id INT,
	@p_first_name NVARCHAR(50),
	@p_last_name NVARCHAR(50),
	@p_email NVARCHAR(100),
	@p_city NVARCHAR(60),
	@p_state NVARCHAR(60),
	@p_customer_segment NVARCHAR(20)
as
begin 
	set nocount on;

	declare @today DATE = CAST(GETDATE() AS DATE);
	declare @expire DATE = DATEADD(DAY, -1, @today);
	declare @max_date DATE = '9999-12-31';

	begin transaction;
	begin try
	-- check if customer exists and has changes in tracked column
		if exists (
			select 1 from dim_customer 
			where customer_id = @p_customer_id and is_current = 1
			and (city <> @p_city or state <> @p_state or customer_segment <> @p_customer_segment)
		)
				
		begin 
			-- S1: expire the current record
			update dim_customer set is_current = 0, expiry_date = @expire, updated_at = GETDATE()
			where customer_id = @p_customer_id and is_current = 1;
					
			-- S2: insert new version
			insert into dim_customer (
				customer_id, first_name, last_name, email, city, state, customer_segment,
				effective_date, expiry_date, is_current
			) values (
				@p_customer_id, @p_first_name, @p_last_name, @p_email, @p_city, @p_state, @p_customer_segment,
				@today, @max_date, 1
			);

		end

		-- if customer doesnt exists at all, perform initial insert
		else if not exists (
			select 1 from dim_customer where customer_id = @p_customer_id
		)
		begin
			insert into dim_customer (
				customer_id, first_name, last_name, email, city, state, customer_segment,
				effective_date, expiry_date, is_current
			) values (
				@p_customer_id, @p_first_name, @p_last_name, @p_email, @p_city, @p_state, @p_customer_segment,
				@today, @max_date, 1
			);
		end

		-- soft update (ie. email only)
		else
		begin 
			update dim_customer set email = @p_email, updated_at = GETDATE()
			where customer_id = @p_customer_id and is_current = 1;
		end

		commit transaction;
	end try
	begin catch
		rollback transaction;
		throw;
	end catch
end;

exec sp_insert_customer_scd2
	@p_customer_id = 20,
	@p_first_name = 'Pankaj',
	@p_last_name = 'Uday',
	@p_email = 'customer20@gmail.com',
	@p_city = 'Delhi',
	@p_state = 'Delhi',
	@p_customer_segment = 'VIP';


select * from dim_customer where is_current = 1 order by customer_id;