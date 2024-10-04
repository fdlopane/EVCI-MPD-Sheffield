# The keys of the inputs and outputs dictionaries, as well as the file names should follow the camelCase notation.

inputs = {}
inputs["MobilePhoneData_EW"] = "../../../data/Mobile-phone-data/2021Nov_trj_oa.parquet" # This is a 5.6Gb file
inputs["Sheffield_OA_geojson"] = "./input-data/Sheffield_OA.geojson"

outputs = {}
outputs["MobilePhoneData_Sheffield"] = "./output-data/2021Nov_trj_oa_Sheffield.parquet"