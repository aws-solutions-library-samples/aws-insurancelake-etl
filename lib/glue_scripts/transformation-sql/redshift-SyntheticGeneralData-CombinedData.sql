/* Amazon Redshift Spectrum View via AWS Glue Catalog */
CREATE OR REPLACE VIEW "general_insurance_redshift_spectrum" AS 
SELECT
  policynumber "Policy Number"
, CAST(startdate AS date) "Summary Date"
, CAST(effectivedate AS date) "Policy Effective Date"
, CAST(expirationdate AS date) "Policy Expiration Date"
, insuredcompanyname "Company"
, lobcode "Line of Business"
, neworrenewal "New or Renewal"
, insuredindustry "Industry"
, insuredsector "Sector"
, distributionchannel "Distribution Channel"
, insuredcity "City"
, insuredstatecode "State"
, insurednumberofemployees "Number of Employees"
, insuredemployeetier "Employer Size Tier"
, insuredannualrevenue "Revenue"
, territory "Territory"
, accidentyeartotalincurredamount "Claim Amount"
, policyinforce "Policy In-force"
, expiringpolicy "Policy Expiring"
, expiringpremiumamount "Premium Expiring"
, writtenpremiumamount "Written Premium"
, writtenpolicy "Written Policy"
, earnedpremium "Earned Premium"
, agentname "Agent Name"
, producercode "Agent Code"
FROM
  awsdatacatalog.syntheticgeneraldata_consume.policydata
WITH NO SCHEMA BINDING
;

/* Amazon Redshift materialized view using external schema

   Depends on external schema to be created as:
		CREATE EXTERNAL SCHEMA IF NOT EXISTS "datalake_syntheticgeneraldata_consume"
		FROM DATA CATALOG
		DATABASE 'syntheticgeneraldata_consume'
		IAM_ROLE default;
*/
DROP MATERIALIZED VIEW IF EXISTS "general_insurance_redshift_materialized"
;

CREATE MATERIALIZED VIEW "general_insurance_redshift_materialized"
BACKUP NO
DISTKEY ( policynumber )
AUTO REFRESH NO
AS
SELECT *
FROM datalake_syntheticgeneraldata_consume.policydata
;