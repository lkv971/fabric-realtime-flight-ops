CREATE PROC Aviation.InsertFlightsProc 
AS 
BEGIN
    UPDATE AP
    SET AP.IATACode = SAP.IATACode,
        AP.ICAOCode = SAP.ICAOCode,
        AP.AirportName = SAP.AirportName,
        AP.Latitude = SAP.Latitude,
        AP.Longitude = SAP.Longitude,
        AP.Elevation = SAP.Elevation,
        AP.TimeZone = SAP.TimeZone,
        AP.Country = SAP.Country,
        AP.City = SAP.City,
        AP.State = SAP.State
    FROM Aviation.Airports AS AP
    INNER JOIN Aviation.StagingAirports AS SAP
    ON AP.AirportKey = SAP.AirportKey
    WHERE AP.IATACode <> SAP.IATACode
          OR AP.ICAOCode <> SAP.ICAOCode
          OR AP.AirportName <> SAP.AirportName
          OR AP.Latitude <> SAP.Latitude
          OR AP.Longitude <> SAP.Longitude
          OR AP.Elevation <> SAP.Elevation
          OR AP.TimeZone <> SAP.TimeZone
          OR AP.Country <> SAP.Country
          OR AP.City <> SAP.City
          OR AP.State <> SAP.State
          ;

    INSERT INTO Aviation.Airports (AirportKey, IATACode, ICAOCode, AirportName, Latitude, Longitude, Elevation, TimeZone, Country, City, State)
    SELECT SAP.AirportKey, SAP.IATACode, SAP.ICAOCode, SAP.AirportName, SAP.Latitude, SAP.Longitude, SAP.Elevation, SAP.TimeZone, SAP.Country, SAP.City, SAP.State
    FROM Aviation.StagingAirports AS SAP
    WHERE NOT EXISTS (
    SELECT 1 
    FROM Aviation.Airports AS AP
    WHERE AP.AirportKey = SAP.AirportKey
    );


    UPDATE AI 
    SET AI.IATACode = SAI.IATACode,
       AI.ICAOCode = SAI.ICAOCode,
       AI.AirlineName = SAI.AirlineName,
       AI.Country = SAI.Country
    FROM Aviation.Airlines AS AI
    INNER JOIN Aviation.StagingAirlines AS SAI 
    ON AI.AirlineKey = SAI.AirlineKey
    WHERE AI.IATACode <> SAI.IATACode
          OR AI.ICAOCode <> SAI.ICAOCode
          OR AI.AirlineName <> SAI.AirlineName
          OR AI.Country <> SAI.Country

    INSERT INTO Aviation.Airlines (AirlineKey, IATACode, ICAOCode, AirlineName, Country)
    SELECT SAI.AirlineKey, SAI.IATACode, SAI.ICAOCode, SAI.AirlineName, SAI.Country
    FROM Aviation.StagingAirlines AS SAI
    WHERE NOT EXISTS (
    SELECT 1
    FROM Aviation.Airlines AS AI
    WHERE AI.AirlineKey = SAI.AirlineKey
    );


    INSERT INTO Aviation.Dates (DateKey, Date, Year, Month, Day, MonthName, DayName)
    SELECT SAD.DateKey, SAD.Date, SAD.Year, SAD.Month, SAD.Day, SAD.MonthName, SAD.DayName
    FROM Aviation.StagingDates AS SAD 
    WHERE NOT EXISTS (
    SELECT 1
    FROM Aviation.Dates AS AD
    WHERE AD.DateKey = SAD.DateKey
          AND AD.Date = SAD.Date
    );

    DELETE AF
    FROM Aviation.Flights AS AF
    INNER JOIN Aviation.StagingFlights AS SAF
    ON AF.FlightDate = SAF.FlightDate
    AND AF.FlightNumber = SAF.FlightNumber;

    INSERT INTO Aviation.Flights (IngestedAt, FlightDate, ScheduledDeparture, ScheduledArrival, DepartureDelay, ArrivalDelay, DepartureAirportKey, ArrivalAirportKey, AirlineKey, FlightNumber, Status)
    SELECT SAF.IngestedAt, SAF.FlightDate, SAF.ScheduledDeparture, SAF.ScheduledArrival, SAF.DepartureDelay, SAF.ArrivalDelay, SAF.DepartureAirportKey, SAF.ArrivalAirportKey, SAF.AirlineKey, SAF.FlightNumber, SAF.Status
    FROM Aviation.StagingFlights AS SAF
    WHERE NOT EXISTS (
    SELECT 1
    FROM Aviation.Flights AS AF
    WHERE AF.FlightDate = SAF.FlightDate
          AND AF.FlightNumber = SAF.FlightNumber
    );

END;