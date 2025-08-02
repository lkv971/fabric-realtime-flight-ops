-- Auto Generated (Do not modify) 328608FF9280A01D2321C016ACCFD2052D527DFC3FFEC43C9153242D91CD4D43
CREATE VIEW Aviation.StagingAirports
AS SELECT DISTINCT AirportKey, IATACode, ICAOCode, AirportName, Latitude, Longitude, Elevation, TimeZone, Country, City, State
FROM LH_Flights.dbo.dimairports;