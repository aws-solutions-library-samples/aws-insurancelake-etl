CREATE TABLE policysummarydata AS
SELECT
  startdate
, enddate
, policynumber
, effectivedate
, expirationdate
, lobcode
, insuredcompanyname
, insuredcity
, insuredstatecode
, insuredindustry
, insuredsector
, insurednumberofemployees
, insuredemployeetier
, insuredannualrevenue
, neworrenewal
, territory
, distributionchannel
, producercode
, agentname
, policyinforce
, writtenpremiumamount
, earnedpremium

, CASE WHEN policymonthindex = 1 THEN 1 ELSE 0
    END AS writtenpolicy

, CASE WHEN year(expirationdate) = year(startdate) AND month(expirationdate) = month(startdate) THEN 1 ELSE 0
    END AS expiringpolicy

, CASE WHEN year(expirationdate) = year(startdate) AND month(expirationdate) = month(startdate) THEN writtenpremiumamount ELSE 0
    END AS expiringpremiumamount

, execution_id
, year
, month
, day

FROM
  syntheticgeneraldata.writtenpolicydata
ORDER BY startdate ASC, lobcode ASC, agentname ASC, insuredindustry ASC