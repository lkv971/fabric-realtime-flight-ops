CREATE TABLE [Aviation].[Flights] (

	[FlightDate] datetime2(3) NULL, 
	[Status] varchar(100) NULL, 
	[DepartureIATA] varchar(100) NULL, 
	[DepartureSched] datetime2(3) NULL, 
	[DepartureDelay] varchar(50) NULL, 
	[ArrivalIATA] varchar(100) NULL, 
	[ArriivalSched] datetime2(3) NULL, 
	[ArrivalDelay] varchar(50) NULL, 
	[Airline] varchar(100) NULL, 
	[FlightNumber] varchar(50) NULL, 
	[IngestedAt] datetime2(5) NULL
);