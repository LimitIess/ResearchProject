import os
import pandas as pd
from defined import *

def folder_creation(file_name):
    """Create folder to store data"""
    if not os.path.exists("transformed_data"):
        os.mkdir("transformed_data")
    if not os.path.exists(f"transformed_data/{file_name}"):
        os.mkdir(f"transformed_data/{file_name}")

def file_find_differences(name_of_file: str):
    """Compare files and creates differences file
    Input: name_of_file: name of file to compare
    Output: csv file with differences
    """
    #Create empty folder to store data
    folder_creation(f"{name_of_file}")
    #Get all folders in data (From defined)
    list_of_folder = sorted(folders, key=lambda x: int(x), reverse=True)
    df1 = pd.read_csv(f"data/{list_of_folder[0]}/{name_of_file}.csv", sep=',', encoding='utf-8')
    #Create empty dataframe to store results
    columns_of_df1 = list(df1.columns)
    tmp = columns_of_df1[-1]
    #Remove the last column
    columns_of_df1 = columns_of_df1[:-1]
    #Add new columns
    columns_of_df1.append(tmp+ "_From")
    columns_of_df1.append(tmp+ "_To")
    columns_of_df1.append("Datum_för_ändring")
    results = pd.DataFrame(columns=columns_of_df1) 

    #Loop through all folders and compare data
    for comparing_folder in list_of_folder[1:]:
        try:
            df2 = pd.read_csv(f"data/{comparing_folder}/{name_of_file}.csv", sep=',', encoding='utf-8')
        except:
            continue
        #Get the differences between both dataframes
        different_values = compare_textfiles(data1 = df1, data2 = df2, date=comparing_folder, columns_list= columns_of_df1)
        results = pd.concat([results, different_values], ignore_index=True)
        df1 = df2.copy()
        print(f"{comparing_folder} is done")
    results.to_csv(f"transformed_data/{name_of_file}/changes_{name_of_file}.csv", index=False)

def compare_textfiles(data1: pd.DataFrame, data2: pd.DataFrame, date: int, columns_list: list):
    """
    Compare two textfiles and return the differences
    
    Input: data1: dataframe to compare
              data2: dataframe to compare
              date: date of the comparison
              columns_list: list of columns to compare
    """
    merged_data = data2.merge(data1, on=columns_list[:-3], how="inner", suffixes=["_From", "_To"])
    
    # Convert the relevant columns to numeric if necessary
    merged_data[columns_list[-3]] = pd.to_numeric(merged_data[columns_list[-3]], downcast="integer", errors="coerce")
    merged_data[columns_list[-2]] = pd.to_numeric(merged_data[columns_list[-2]], downcast="integer", errors="coerce")

    # Compare rows that are different in both dataframes
    different_dataframe = merged_data[merged_data[columns_list[-3]] != merged_data[columns_list[-2]]]
    different_dataframe = different_dataframe[columns_list[:-1]]
    different_dataframe[columns_list[-1]] = date
    
    return different_dataframe

    