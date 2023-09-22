SELECT	syntheticlifedata.policydata.policyid,
		policyquotationdate,
		syntheticlifedata.policydata.policyeffectivedate,
		syntheticlifedata.policydata.firstname,
		syntheticlifedata.policydata.lastname,
		issuestate,
		city,
		country,
		productcode,
		newrenew,
		policystatus,
		policyterminationdate,
		policyterminationreason,
		issueage,
		customerno,
		customerdob,
		personalid,
		gender,
		emailid,
		phoneno,
		smokingclass,
		uwclass,
		coverageunit,
		syntheticlifedata.policydata.beneficiaryname,
		syntheticlifedata.policydata.beneficiaryrelationship,
		syntheticlifedata.policydata.beneficiarypct,
		policyterm,
		yearlypremium,
		sumassured,
		monthlypremium,
		producer,
		producercommision,
		premiumpaidtilldate,
		commisionpaidtilldate,
		claimamount,
		claimdescription,
		claimno,
		paidamount,
		syntheticlifedata.policydata.lastupdated,
		syntheticlifedata.policydata.execution_id,
		syntheticlifedata.policydata.year,
		syntheticlifedata.policydata.month,
		syntheticlifedata.policydata.day

FROM	syntheticlifedata.policydata
		LEFT OUTER JOIN syntheticlifedata.claimdata
		ON syntheticlifedata.policydata.policyid = syntheticlifedata.claimdata.policyid
	

ORDER BY syntheticlifedata.policydata.policyid ASC
		

