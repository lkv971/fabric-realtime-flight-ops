-- Auto Generated (Do not modify) 9AE344B8F65E8F3EAB8EE08929151439A5503EB26A07407531A73562B13DEFD3


CREATE VIEW Aviation.StagingDates
AS SELECT DISTINCT DateKey, Date, Year, Month, Day, MonthName, DayName
FROM LH_Flights.dbo.dimdates;