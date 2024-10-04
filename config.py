# The keys of the inputs and outputs dictionaries, as well as the file names should follow the camelCase notation.

inputs = {}
inputs["MobilePhoneData_EW"] = "../../../data/Mobile-phone-data/2021Nov_trj_oa.parquet" # This is a 5.6Gb file
inputs["Sheffield_OA_geojson"] = "./input-data/Sheffield_OA.geojson"

outputs = {}
outputs["MobilePhoneData_Sheffield_parquet"] = "./output-data/2021Nov_trj_oa_Sheffield.parquet"
outputs["MobilePhoneData_Sheffield_csv"] = "./output-data/2021Nov_trj_oa_Sheffield.csv"
outputs["Daily_casual_activities"] = "./output-data/daily_casual_activities.csv"