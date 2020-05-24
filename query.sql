SELECT DISTINCT Company, hr as Hour, highp as HighestPrice, ts as HighPriceOccurance
FROM (SELECT name as Company,substring(ts,12,2) as hr,MAX(high) as highp
FROM  yfinance_data 
GROUP BY name, substring(ts,12,2)) m JOIN (SELECT name,substring(ts,12,2) as hour, high,ts FROM yfinance_data) s 
ON m.Company = s.name AND m.hr = s.hour AND m.highp = s.high
ORDER BY Company,Hour;
