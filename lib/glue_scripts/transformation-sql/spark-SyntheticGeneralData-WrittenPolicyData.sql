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
, execution_id
, year
, month
, day
FROM
  syntheticgeneraldata.writtenpolicydata
ORDER BY startdate ASC, lobcode ASC, agentname ASC, insuredindustry ASC