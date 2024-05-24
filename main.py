import requests
import pandas as pd
import json
import os
from transformations import file_find_differences
from convert_to_csv import excel_convert_to_csv, txt_convert_to_csv
from collect_data import collect_data_from_API, collect_inhabitant_per_municipality
from interpolation import read_data_and_interpolate

#Create main
def main():
    #Collect data from API
    collect_data_from_API()
    #Convert already collected data to csv
    excel_convert_to_csv()
    #Convert already collected txt files to csv
    txt_convert_to_csv()
    #Collect data about number of inhabitants
    collect_inhabitant_per_municipality()
    #Interpolate values
    read_data_and_interpolate()
    #Find differences between files
    file_find_differences("acov19dag")
    file_find_differences("bcov19Kom")
    file_find_differences("ccov19kon")
    file_find_differences("ccov19Reg")
    file_find_differences("ccov19Regsasong")
    file_find_differences("dcov19ald")
    file_find_differences("ecov19sabo")
    file_find_differences("ecov19sabosasong")
    file_find_differences("PCRtestVAr_k")
    file_find_differences("PCRtestVAr_m")
    file_find_differences("PCRtestVAr_s")
    file_find_differences("xcov19ivavDAG")
    file_find_differences("ycov19ivavald")
    file_find_differences("ycov19ivavkon")
    
if __name__ == "__main__":
    main()