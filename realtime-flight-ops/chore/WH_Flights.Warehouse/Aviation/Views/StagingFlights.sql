-- Auto Generated (Do not modify) DD0F614ECD3AB5C492B238134F5EE24CE7277AF5DFA0DF0B6A51C03781751C0C


CREATE VIEW Aviation.StagingFlights
AS SELECT DISTINCT IngestedAt, FlightDate, ScheduledDeparture, ScheduledArrival, DepartureDelay, ArrivalDelay, DepartureAirportKey, ArrivalAirportKey, AirlineKey, FlightNumber, Status
FROM LH_Flights.dbo.factflights;