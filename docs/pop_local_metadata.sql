--Script to get the local metadata for population health metrics
SELECT DISTINCT 
	[Indicator_ID] AS [indicator_id],
    CAST([Date_updated] AS date) AS [date_updated]
FROM [Data_Lab_NCL_Dev].[GrahamR].[Monthly_Population_Health]
WHERE Indicator_ID != ''