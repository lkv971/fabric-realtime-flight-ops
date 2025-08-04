CREATE TABLE [Aviation].[IngestionLogs] (

	[TableName] varchar(100) NULL, 
	[ProcessedTime] datetime2(3) NULL, 
	[RecordCount] int NULL, 
	[Status] varchar(100) NULL
);