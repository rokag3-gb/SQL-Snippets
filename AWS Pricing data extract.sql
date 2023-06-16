use sale;
--BATCH_ETL_BLOB_EXTERNAL_DATA

drop table if exists #Data;

create table #Data (
	ExData NVARCHAR(MAX)
);

DECLARE	@ExData NVARCHAR(MAX) = ''
	, @Path NVARCHAR(1000)
--SELECT	@Path = '202306' + '/' + 'aws_ec2_pricing_us-east-1.json';
SELECT	@Path = '202306' + '/' + 'aws_ec2_pricing.json';
EXEC DBO.USP_GET_BLOB_EXTERNAL_DATA @Path = @Path, @ExData = @ExData OUTPUT;

--PRINT LEN(@ExData);

insert into #Data (ExData) values (@ExData);

select len(ExData), * from #Data; -- 276,685,837

/*
{
  "ErrorMessage": "[7119] 허용되는 최대 크기 2147483647바이트를 초과하여 LOB를 확대하려고 했습니다. (Proc: DBO.USP_GET_BLOB_EXTERNAL_DATA , line: 51)",
  "SQL": "SELECT BULKDATA = CONVERT(NVARCHAR(MAX),    COALESCE(BulkColumn, '' COLLATE Cyrillic_General_100_CI_AS_SC_UTF8) -- any UTF8 collation    ) COLLATE Korean_Wansung_CI_AS -- you default database collation  FROM OPENROWSET(   BULK N'202306/aws_ec2_pricing.json'   , DATA_SOURCE = 'AzureBlob.stor1airflow1etl1target.airflow-out'   --, SINGLE_CLOB -- ASCII로 읽은 후 현재 데이터베이스의 데이터 정렬을 사용하여 내용을 varchar(max) 형식의 단일 행 및 단일 열로 된 행 집합으로 반환   , SINGLE_BLOB -- varbinary(max) 형식의 단일 행 및 단일 열로 된 행 집합으로 반환   --, CODEPAGE = '65001' -- Unicode. SQL Server에서의 Unicode 는 UTF-16 LE(Little Endian)를 의미함.  ) AS LOB;"
}
*/

/*************************************************************************/

select	offerCode = json_value(d.ExData, '$.offerCode')
	, products = json_query(d.ExData, '$.products')
	, p.productCount
	, terms = json_query(d.ExData, '$.terms')
	, t.termCount
from	#Data d
	cross apply (
	select	productCount = count(1)
	from	openjson(json_query(d.ExData, '$.products'))
	) p
	cross apply (
	select	termCount = count(1)
	from	openjson(json_query(d.ExData, '$.terms'))
	) t
;
return;


select	offerCode = json_value(d.ExData, '$.offerCode')
	, productKey = p.[key]
	, sku = json_value(p.value, '$.sku')
	, productFamily = json_value(p.value, '$.productFamily')
	, servicecode = json_value(p.value, '$.attributes.servicecode')
	, location = json_value(p.value, '$.attributes.location')
	, locationType = json_value(p.value, '$.attributes.locationType')
	, instanceType = json_value(p.value, '$.attributes.instanceType')
	, currentGeneration = json_value(p.value, '$.attributes.currentGeneration')
	, instanceFamily = json_value(p.value, '$.attributes.instanceFamily')
	, vcpu = json_value(p.value, '$.attributes.vcpu')
	, physicalProcessor = json_value(p.value, '$.attributes.physicalProcessor')
	, clockSpeed = json_value(p.value, '$.attributes.clockSpeed')
	, memory = json_value(p.value, '$.attributes.memory')
	, storage = json_value(p.value, '$.attributes.storage')
	, networkPerformance = json_value(p.value, '$.attributes.networkPerformance')
	, processorArchitecture = json_value(p.value, '$.attributes.processorArchitecture')
	, tenancy = json_value(p.value, '$.attributes.tenancy')
	, operatingSystem = json_value(p.value, '$.attributes.operatingSystem')
	, licenseModel = json_value(p.value, '$.attributes.licenseModel')
	, usagetype = json_value(p.value, '$.attributes.usagetype')
	, operation = json_value(p.value, '$.attributes.operation')
	, availabilityzone = json_value(p.value, '$.attributes.availabilityzone')
	, capacitystatus = json_value(p.value, '$.attributes.capacitystatus')
	, classicnetworkingsupport = json_value(p.value, '$.attributes.classicnetworkingsupport')
	, ecu = json_value(p.value, '$.attributes.ecu')
	, enhancedNetworkingSupported = json_value(p.value, '$.attributes.enhancedNetworkingSupported')
	, gpuMemory = json_value(p.value, '$.attributes.gpuMemory')
	, instancesku = json_value(p.value, '$.attributes.instancesku')
	, intelAvxAvailable = json_value(p.value, '$.attributes.intelAvxAvailable')
	, intelAvx2Available = json_value(p.value, '$.attributes.intelAvx2Available')
	, intelTurboAvailable = json_value(p.value, '$.attributes.intelTurboAvailable')
	, marketoption = json_value(p.value, '$.attributes.marketoption')
	, normalizationSizeFactor = json_value(p.value, '$.attributes.normalizationSizeFactor')
	, preInstalledSw = json_value(p.value, '$.attributes.preInstalledSw')
	, processorFeatures = json_value(p.value, '$.attributes.processorFeatures')
	, regionCode = json_value(p.value, '$.attributes.regionCode')
	, servicename = json_value(p.value, '$.attributes.servicename')
	, vpcnetworkingsupport = json_value(p.value, '$.attributes.vpcnetworkingsupport')
--into	#products
from	#Data d
	cross apply openjson(json_query(d.ExData, '$.products')) p
;


select	offerCode = json_value(d.ExData, '$.offerCode')
	, term_kind = t.[key]
	, termsKey = o.[key]
	, offerTermFullCode = u.[key]
	, offerTermCode = json_value(u.value, '$.offerTermCode')
	, sku = json_value(u.value, '$.sku')
	, effectiveDate = json_value(u.value, '$.effectiveDate')
	--, priceDimensions_key = p.[key]
	, priceDimensions_rateCode = json_value(p.value, '$.rateCode')
	, priceDimensions_description = json_value(p.value, '$.description')
	, priceDimensions_beginRange = json_value(p.value, '$.beginRange')
	, priceDimensions_endRange = json_value(p.value, '$.endRange')
	, priceDimensions_unit = json_value(p.value, '$.unit')
	, priceDimensions_pricePerUnit_USD = json_value(p.value, '$.pricePerUnit.USD')
	, priceDimensions_appliesTo = json_query(p.value, '$.appliesTo')
	, termAttributes = json_query(u.value, '$.termAttributes')
into	#terms
from	#Data d
	cross apply openjson(json_query(d.ExData, '$.terms')) t -- "OnDemand" depth
	cross apply openjson(json_query(t.value, '$')) o -- "4YS58GA5FM68MBNC" depth
	cross apply openjson(json_query(o.value, '$')) u -- "4YS58GA5FM68MBNC.JRTCKXETXF" depth
	cross apply openjson(json_query(u.value, '$.priceDimensions')) p -- "priceDimensions" depth
--where	trim(o.[key]) = 'XBEH8V3KRTK6J6TT'
;