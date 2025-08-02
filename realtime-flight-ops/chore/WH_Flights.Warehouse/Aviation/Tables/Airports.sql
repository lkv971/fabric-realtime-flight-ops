CREATE TABLE [Aviation].[Airports] (

	[AirportKey] int NULL, 
	[IATACode] varchar(50) NULL, 
	[ICAOCode] varchar(50) NULL, 
	[AirportName] varchar(100) NULL, 
	[Latitude] float NULL, 
	[Longitude] float NULL, 
	[Elevation] int NULL, 
	[TimeZone] varchar(100) NULL, 
	[Country] varchar(100) NULL, 
	[City] varchar(100) NULL, 
	[State] varchar(100) NULL
);