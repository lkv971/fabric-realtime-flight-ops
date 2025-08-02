CREATE TABLE [Aviation].[Flights] (

	[IngestedAt] datetime2(5) NULL, 
	[FlightDate] date NULL, 
	[ScheduledDeparture] datetime2(3) NULL, 
	[ScheduledArrival] datetime2(3) NULL, 
	[DepartureDelay] int NULL, 
	[ArrivalDelay] int NULL, 
	[DepartureAirportKey] int NULL, 
	[ArrivalAirportKey] int NULL, 
	[AirlineKey] int NULL, 
	[FlightNumber] varchar(50) NULL, 
	[Status] varchar(50) NULL
);