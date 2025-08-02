-- Auto Generated (Do not modify) 08335F3AA19F599BDC7E2E2CCD70B825DCA6A8E0C9220DBCA914FC9CEB95619A
CREATE VIEW Aviation.StagingAirlines
AS SELECT DISTINCT AirlineKey, IATACode, ICAOCode, AirlineName, Country
FROM LH_Flights.dbo.dimairlines;