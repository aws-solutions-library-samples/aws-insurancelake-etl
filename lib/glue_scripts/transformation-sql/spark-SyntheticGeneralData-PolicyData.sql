SELECT
  syntheticgeneraldata.policydata.startdate
, syntheticgeneraldata.policydata.enddate
, syntheticgeneraldata.policydata.policynumber
, effectivedate
, expirationdate
, lobcode
, customerno
, insuredcompanyname
, ein
, insuredcity
, insuredstatecode
, insuredcontactcellphone
, insuredcontactemail
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
, accidentyeartotalincurredamount
, policyinforce
, expiringpolicy
, expiringpremiumamount
, writtenpremiumamount
, writtenpolicy
, earnedpremium
, syntheticgeneraldata.policydata.execution_id
, syntheticgeneraldata.policydata.year
, syntheticgeneraldata.policydata.month
, syntheticgeneraldata.policydata.day
FROM
  syntheticgeneraldata.policydata
LEFT OUTER JOIN
  syntheticgeneraldata.claimdata
  ON syntheticgeneraldata.policydata.policynumber = syntheticgeneraldata.claimdata.policynumber
  AND syntheticgeneraldata.policydata.startdate = syntheticgeneraldata.claimdata.startdate
ORDER BY syntheticgeneraldata.policydata.startdate ASC, lobcode ASC, agentname ASC, insuredindustry ASC