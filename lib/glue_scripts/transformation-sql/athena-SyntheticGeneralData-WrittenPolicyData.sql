CREATE OR REPLACE VIEW policysummarydata_new AS 
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
, neworrenewal
, territory
, distributionchannel
, policyinforce
, writtenpremiumamount
, earnedpremium
, execution_id
FROM
  syntheticgeneraldata_consume.policysummarydata
WHERE neworrenewal = 'New'
ORDER BY startdate ASC, lobcode ASC, agentname ASC, insuredindustry ASC;

CREATE OR REPLACE VIEW policysummarydata_renewal AS 
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
, neworrenewal
, territory
, distributionchannel
, policyinforce
, writtenpremiumamount
, earnedpremium
, execution_id
FROM
  syntheticgeneraldata_consume.policysummarydata
WHERE neworrenewal = 'Renewal'
ORDER BY startdate ASC, lobcode ASC, agentname ASC, insuredindustry ASC;