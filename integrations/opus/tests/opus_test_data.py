opus_file_1 = """<?xml version="1.0" encoding="utf-8"?>
<kmd>
 <orgUnit id="1" client="813" lastChanged="2020-09-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit/>
  <shortName>AND.K</shortName>
  <longName>Andeby Kommune</longName>
  <street>Paradisæblevej 1</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>9109384</phoneNumber>
  <cvrNr>1829458</cvrNr>
  <eanNr>345839</eanNr>
  <seNr>39583495</seNr>
  <pNr>0000000000</pNr>
  <orgType>00000</orgType>
  <orgTypeTxt>Organisation</orgTypeTxt>
 </orgUnit>
 <orgUnit id="2" client="813" lastChanged="2020-09-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit>1</parentOrgUnit>
  <shortName>Underenhed</shortName>
  <longName>Underenhed 1</longName>
  <street>Paradisæblevej 2</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>94583958</phoneNumber>
  <cvrNr>3459345788</cvrNr>
  <eanNr>234528367</eanNr>
  <seNr>34590365</seNr>
  <pNr>923458347</pNr>
  <costCenter>345300000000</costCenter>
  <orgType>00001</orgType>
  <orgTypeTxt>Direktion</orgTypeTxt>
 </orgUnit>
 <orgUnit id="3" client="813" lastChanged="2015-01-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit>2</parentOrgUnit>
  <shortName>U-U-Enh</shortName>
  <longName>Under-under-enhed</longName>
  <street>Paradisæblevej 3</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>89345738</phoneNumber>
  <cvrNr>9347358</cvrNr>
  <eanNr>82346427</eanNr>
  <seNr>834738</seNr>
  <pNr>9345738</pNr>
  <costCenter>32492430</costCenter>
  <orgType>00002</orgType>
  <orgTypeTxt>Center</orgTypeTxt>
 </orgUnit>
  <employee id="1000" client="813" lastChanged="2020-10-01">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0101010000</cpr>
  <firstName>Fornavn</firstName>
  <lastName>Efteravn</lastName>
  <address>Testvej 10</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Ansat</workContractText>
  <positionId>500</positionId>
  <position>Ansat</position>
  <positionShort>TESTER</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
   <employee id="1001" client="813" lastChanged="2020-01-19">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0101010000</cpr>
  <firstName>Fornavn1</firstName>
  <lastName>Efteravn2</lastName>
  <address>Testvej 10</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Ansat</workContractText>
  <positionId>500</positionId>
  <position>Ansat</position>
  <positionShort>TESTER</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
    <employee id="1002" client="813" lastChanged="2020-01-19">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0101010000</cpr>
  <firstName>Fornavn1</firstName>
  <lastName>Efteravn2</lastName>
  <address>Testvej 10</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Ansat</workContractText>
  <positionId>500</positionId>
  <position>Ansat</position>
  <positionShort>TESTER</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
     <employee id="1006" client="813" lastChanged="2020-01-19">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0201010000</cpr>
  <firstName>Fornavn2</firstName>
  <lastName>Efteravn3</lastName>
  <address>Testvej 11</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Annulleret</workContractText>
  <positionId>500</positionId>
  <position>Annulleret</position>
  <positionShort>Annulleret</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
  <employee id="999" client="813" action="leave"/>
</kmd>
"""

opus_file_2 = """<?xml version="1.0" encoding="utf-8"?>
<kmd>
 <orgUnit id="1" client="813" lastChanged="2020-09-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit/>
  <shortName>AND.K</shortName>
  <longName>Andeby Kommune</longName>
  <street>Paradisæblevej 1</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>9109384</phoneNumber>
  <cvrNr>1829458</cvrNr>
  <eanNr>345839</eanNr>
  <seNr>39583495</seNr>
  <pNr>0000000000</pNr>
  <orgType>00000</orgType>
  <orgTypeTxt>Organisation</orgTypeTxt>
 </orgUnit>
 <orgUnit id="2" client="813" lastChanged="2020-09-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit>1</parentOrgUnit>
  <shortName>Underenhed</shortName>
  <longName>Underenhed 1</longName>
  <street>Paradisæblevej 2</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>94583958</phoneNumber>
  <cvrNr>3459345788</cvrNr>
  <eanNr>234528367</eanNr>
  <seNr>34590366</seNr>
  <pNr>923458347</pNr>
  <costCenter>345300000000</costCenter>
  <orgType>00001</orgType>
  <orgTypeTxt>Direktion</orgTypeTxt>
 </orgUnit>
 <orgUnit id="3" client="813" lastChanged="2015-01-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit>1</parentOrgUnit>
  <shortName>U-U-Enh</shortName>
  <longName>Under-under-enhed</longName>
  <street>Paradisæblevej 3</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>89345738</phoneNumber>
  <cvrNr>9347358</cvrNr>
  <eanNr>82346427</eanNr>
  <seNr>834738</seNr>
  <pNr>9345738</pNr>
  <costCenter>32492430</costCenter>
  <orgType>00002</orgType>
  <orgTypeTxt>Center</orgTypeTxt>
 </orgUnit>
  <orgUnit id="4" client="813" lastChanged="2020-01-16">
  <startDate>1900-01-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit>1</parentOrgUnit>
  <shortName>U-U-Enh2</shortName>
  <longName>2. Under-under-enhed</longName>
  <street>Paradisæblevej 4</street>
  <zipCode>8880</zipCode>
  <city>Andeby</city>
  <phoneNumber>89345738</phoneNumber>
  <cvrNr>9347358</cvrNr>
  <eanNr>82346427</eanNr>
  <seNr>834738</seNr>
  <pNr>9345738</pNr>
  <costCenter>32492430</costCenter>
  <orgType>00002</orgType>
  <orgTypeTxt>TestCenter</orgTypeTxt>
 </orgUnit>
  <orgUnit id="5" client="813" lastChanged="2022-03-22">
  <startDate>2022-03-01</startDate>
  <endDate>9999-12-31</endDate>
  <parentOrgUnit>1</parentOrgUnit>
  <shortName>Ny org.enhed</shortName>
  <longName>Ny organisationsenhed</longName>
 </orgUnit>
  <employee id="1000" client="813" lastChanged="2020-10-02">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0101010000</cpr>
  <firstName>Fornavn</firstName>
  <lastName>Efteravn</lastName>
  <address>Testvej 10</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Softwaretester</workContractText>
  <positionId>500</positionId>
  <position>Softwaretester</position>
  <positionShort>TESTER</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
   <employee id="1001" client="813" lastChanged="2021-01-19">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0101010000</cpr>
  <firstName>Fornavn</firstName>
  <lastName>Efteravn</lastName>
  <address>Testvej 10</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Softwaretester</workContractText>
  <positionId>500</positionId>
  <position>Softwaretester</position>
  <positionShort>TESTER</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
     <employee id="1002" client="813" lastChanged="2020-01-19">
  <entryDate/>
  <leaveDate/>
  <cpr suppId="0">0101010000</cpr>
  <firstName>Fornavn1</firstName>
  <lastName>Efteravn2</lastName>
  <address>Testvej 10</address>
  <postalCode>9900</postalCode>
  <city>Frederikshavn</city>
  <country>DK</country>
  <workPhone/>
  <workContract>08</workContract>
  <workContractText>Ansat</workContractText>
  <positionId>500</positionId>
  <position>Ansat</position>
  <positionShort>TESTER</positionShort>
  <isManager>false</isManager>
  <superiorLevel>0</superiorLevel>
  <subordinateLevel>00</subordinateLevel>
  <orgUnit>3</orgUnit>
  <payGradeText>TEST.</payGradeText>
  <numerator>1 </numerator>
  <denominator>1 </denominator>
 </employee>
  <employee id="9991" client="813" action="leave"/>
  <employee id="999" client="813" action="leave"/>
</kmd>
"""
