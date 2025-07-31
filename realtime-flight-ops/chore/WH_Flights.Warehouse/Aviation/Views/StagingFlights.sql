-- Auto Generated (Do not modify) 14FF7CB8B2CF107A255F88F31EA197ABDE77EADEB5D99049B0ACC01C2DE881B5
CREATE VIEW Aviation.StagingFlights
AS SELECT DISTINCT FlightDate, Status, DepartureIATA, DepartureSched, DepartureDelay, ArrivalIATA, ArriivalSched, ArrivalDelay, Airline, FlightNumber, IngestedAt
FROM LH_Flights.dbo.flights