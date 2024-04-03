from pyspark.sql import types

from common.namings import BmrsDataCategory

BMRS_SCHEMAS = {
    BmrsDataCategory.GENERATION: types.StructType([
    types.StructField('bmUnit', types.StringType(), True), 
    types.StructField('halfHourEndTime', types.StringType(), True), 
    types.StructField('nationalGridBmUnitId', types.StringType(), True), 
    types.StructField('quantity', types.DoubleType(), True), 
    types.StructField('settlementDate', types.StringType(), True), 
    types.StructField('settlementPeriod', types.IntegerType(), True), 
]), 
    BmrsDataCategory.CAPACITY: types.StructType([
    types.StructField('bmUnit', types.StringType(), True),
    types.StructField('publishTime', types.StringType(), True),
    types.StructField('effectiveFrom', types.StringType(), True),
    types.StructField('psrType', types.StringType(), True),
    types.StructField('installedCapacity', types.DoubleType(), True),
])
}

POWER_PLANT_ID_SCHEMA = {
    'dictionary_id': int, 
    'name': str, 
    'sett_bmu_id': str, 
    'ngc_bmu_id': str
}