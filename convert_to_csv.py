import pandas as pd
from defined import *
from datetime import date
import numpy as np

def make_dir_excel(date_file: str):
    """ Create a folder to store the data in if it does not exist
    Input:  date_file: date of the file
    """
    #Create date folder
    if not os.path.exists("data/" + date_file):
        os.mkdir("data/" + date_file)
    
def discard_rows(df: pd.DataFrame):
    """Discard rows with specific values in a list
    Input   df: dataframe to discard rows from
    """
    if "SÄBO" or "Hemtjänst" in df:
        df = df[~df.isin(['..']).any(axis=1)]
        df = df[~df.isin(['.']).any(axis=1)]
    elif "Ej bedömbara" in df:
        df = df[~df.isin(['.']).any(axis=1)]
    elif "Okänd tidpunkt" in df:
        df = df[~df.isin(['.']).any(axis=1)]
    for category in categories_to_skip:
        df = df[~df.isin([category]).any(axis=1)]
    return df

def excel_convert_to_csv():
    """Convert excel file to corresponding csv files"""
    #Get all files from a folder
    files = os.listdir("excel/2021_2020")
    for file in files:
        if file.endswith(".xlsx"):
            url =  f"excel/2021_2020/{file}"
            df_excel = pd.read_excel(url, sheet_name=None)
            get_dfs_from_excel(df_excel, file)

    #Get all files from a folder
    files = os.listdir("excel/2022")
    for file in files:
        if file.endswith(".xlsx"):
            url =  f"excel/2022/{file}"
            df_excel = pd.read_excel(url, sheet_name=None)
            get_dfs_from_excel(df_excel, file)

def txt_convert_to_csv():
    """Convert text file to corresponding csv files"""
    #Get all files from a folder
    data = os.listdir("data/")
    for folder in data:
        files = os.listdir(f"data/{folder}")
        for file in files:
            if file.endswith(".txt"):
                url =  f"data/{folder}/{file}"
                #Read a text file and save it as csv
                text_file = pd.read_csv(url, sep='\t', encoding='latin-1').copy()
                #Check if some values exists in a list
                text_file = discard_rows(text_file)
                #Save the csv file
                text_file.to_csv(f"data/{folder}/{file[:-4]}.csv", index=False)
                #Delete the text file
                os.remove(url)
        print(f"Folder - {folder} - Completed")


def get_dfs_from_excel(df: pd.DataFrame, file: str):
    """Get dataframes from excel file and save them as csv files
    Input:  df: dataframe from excel file
            file: file name
    """
    #Create a copy as two functions will use the same dataframe. In order to not change the original dataframe, a copy is created.
    #Delete the .xlsx from the file name
    #Get the last 5 characters from the file name
    name_of_file = file[:-5]
    #Get month, day and year from the file name
    date_of_file = name_of_file[-11:]

    #Convert to date
    date_of_file = pd.to_datetime(date_of_file).date().strftime("%Y%m%d")
    
    #get year from file name
    year_of_file = name_of_file[-4:]

    #Create date folder
    make_dir_excel(date_of_file)

    #Veckodata Kommun_stadsdel and Region may not texist in older files
    if ("Veckodata Region" or "Veckodata Kommun_stadsdel") in df:
        #As some files have no year in the file name, we need to add it manually
        if year_of_file == "2020":
            df["Veckodata Region"]["år"] = 2020
            df["Veckodata Kommun_stadsdel"]["år"] = 2020
        df2 = df["Veckodata Region"].copy()
        convert_to_acov19dag(df["Antal per dag region"]).to_csv("data/"+ date_of_file +"/acov19DAG.csv", sep=',', index=False, encoding="utf-8")
        convert_to_xcov19ivavDAG(df["Antal avlidna per dag"], df["Antal intensivvårdade per dag"]).to_csv("data/"+ date_of_file +"/xcov19ivavDAG.csv", sep=',', index=False, encoding="utf-8")
        convert_to_ccov19regsasong(df2).to_csv("data/"+ date_of_file +"/ccov19regsasong.csv", sep=',', index=False, encoding="utf-8")
        convert_to_bcov19kom(df["Veckodata Kommun_stadsdel"]).to_csv("data/"+ date_of_file +"/bcov19Kom.csv", sep=',', index=False, encoding="utf-8")
        convert_to_ccov19reg(df["Veckodata Region"]).to_csv("data/"+ date_of_file +"/ccov19reg.csv", sep=',', index=False, encoding="utf-8")
        print("Done with " + date_of_file)
    else:
        convert_to_acov19dag(df["Antal per dag region"]).to_csv("data/"+ date_of_file +"/acov19DAG.csv", sep=',', index=False, encoding="utf-8")
        convert_to_xcov19ivavDAG(df["Antal avlidna per dag"], df["Antal intensivvårdade per dag"]).to_csv("data/"+ date_of_file +"/xcov19ivavDAG.csv", sep=',', index=False, encoding="utf-8")
        print("Done with " + date_of_file)
    
def convert_to_acov19dag(df_excel: pd.DataFrame):
    """Convert to type of acov19DAG.csv
    
    Input:    df_excel: dataframe of sheet "Antal per dag region"

    """
    df_excel = discard_rows(df_excel)
    # Melt the DataFrame
    df_melted = pd.melt(df_excel, id_vars=['Statistikdatum'], var_name='Region', value_name='Value')
    #Used as a sort to get in correct order
    df_melted["Region"] = df_melted["Region"].replace("Totalt_antal_fall", "Alla")

    # Create a new DataFrame with Date and Types columns
    result = df_melted.groupby(['Region','Statistikdatum'])['Value'].sum().reset_index()
    #Change name of columns corresponding to csv file
    result.columns = ["Region", "Dag", "Fall per dag"]
    #Change name of "Totalt_antal_fall" to "Riket
    result["Region"] = result["Region"].replace("Alla", "Riket")
    return(result)

def convert_to_xcov19ivavDAG(df_avlidna: pd.DataFrame, df_intensiv_vard: pd.DataFrame):
    """ Combine two dataframes and melt them to one dataframe in form of xcov19ivavDAG.csv
    
    Input:    df_avlidna: dataframe of sheet "Antal avlidna per dag"
                df_intensiv_vard: dataframe of sheet "Antal intensivvårdade per dag"
    """
    #Change name of columns corresponding to the one in csv file
    df_avlidna = discard_rows(df_avlidna)
    df_intensiv_vard = discard_rows(df_intensiv_vard)
    df_avlidna.columns = ["Dag", "Antal avlidna fall"]
    df_intensiv_vard.columns = ["Dag", "Antal intensivvårdade fall"]
    #Convert to date
    #Uppgift saknas - in date cannot be transformed to date
    #Use all observations except last row and convert to date
    df_avlidna["Dag"] = pd.to_datetime(df_avlidna["Dag"].iloc[:-1]).dt.date
    df_intensiv_vard["Dag"] = pd.to_datetime(df_intensiv_vard["Dag"]).dt.date
    #Combine two dataframes
    df_merged = df_intensiv_vard.merge(df_avlidna, on=["Dag"], how="outer")
    #For column "Dag" and value Uppgift saknas replace value with "Okänd tidpunkt"
    df_merged["Dag"] = df_merged["Dag"].replace("Uppgift saknas", pd.to_datetime("2040-10-26").date())
    #When we are merging, the number of rows differ which creates NaN values.
    #df_merged["Dag"] = df_merged["Dag"].replace("NaN", pd.to_datetime("2040-10-26").date())
    df_merged["Dag"] = df_merged["Dag"].fillna(pd.to_datetime("2040-10-26").date())
    df_merged = df_merged.fillna(0)
    # Melt the DataFrame
    df_melted = pd.melt(df_merged, id_vars=['Dag'], var_name='Indikator', value_name='Intensivvårdade respektive avlidna per dag')
    # Create a new DataFrame with Date and Types columns
    #df_melted.to_csv("testing", sep='\t', index=False)
    result = df_melted.groupby(['Indikator','Dag'])['Intensivvårdade respektive avlidna per dag'].sum().reset_index()
    result["Indikator"] = pd.Categorical(result["Indikator"],categories=["Antal intensivvårdade fall","Antal avlidna fall"])
    result=result.sort_values(["Indikator","Dag"]).reset_index(drop=True)
    df_merged["Dag"] = df_merged["Dag"].replace("2040-10-26", "Okänd tidpunkt")
    ## GETTING ERROR TILT
    return(result)

def convert_to_ccov19regsasong(df_reg: pd.DataFrame):
    """Convert to type of ccov19regsasong.csv
    Input:        df_reg: dataframe of sheet "Veckodata Region"
    """
    df_reg = discard_rows(df_reg)
    #THE Years counts to week 25. --> 2019-2020 is til 2020 week 25.
    #all values from week 26 to 2021 week 25 is 2020-2021
    df_reg["år"] = df_reg.apply(lambda x: x["år"] if x["veckonummer"] <= 25 else x["år"]+1, axis=1)

    #To get 3 --> 03, 4 --> 04 etc.
    df_reg["veckonummer"] = df_reg["veckonummer"].apply(lambda x: ("0" + str(x)) if len(str(x)) == 1 else x)
    #To get V 03, V 04 etc.
    df_reg["veckonummer"] = "v " + df_reg["veckonummer"].astype(str)

    #To get years in format 2019-2020, 2020-2021 etc.
    df_reg["år"] = (df_reg["år"].astype(int)-1).astype(str) + "-" + (df_reg["år"].astype(int)).astype(str)


    #Create new dataframe with specific chosen columns
    df_reg = df_reg[["år", "veckonummer", "Region", "Antal_fall_vecka", "Antal_intensivvårdade_vecka", "Antal_avlidna_vecka"]]
    #Change column names
    df_reg.columns = ["År", "Vecka", "Region", "Antal fall", "Antal intensivvårdade fall", "Antal avlidna fall"]
    
    # Melt the DataFrame
    df_melted = pd.melt(df_reg, id_vars=['År', 'Vecka', 'Region'], var_name='Indikator', value_name='Fall, intensivvårdade och avlidna efter region och vecka (säsongsvis).')
    #Groupby
    result = df_melted.groupby(["Region", "Indikator", "Vecka","År"])['Fall, intensivvårdade och avlidna efter region och vecka (säsongsvis).'].sum().reset_index()
    #Change name of files to get the in them same type as in txt file
    result["Region"] = result["Region"].replace("Jämtland Härjedalen", "Jämtland")
    result["Region"] = result["Region"].replace("Sörmland", "Södermland")
    riket = result.groupby(["Indikator", "Vecka","År"])['Fall, intensivvårdade och avlidna efter region och vecka (säsongsvis).'].sum().reset_index()

    #Add riket to results as a new region in dataframe
    riket["Region"] = "Riket"
    
    #Combine two dataframes
    result = pd.concat([result, riket], ignore_index=True)
    #Sort in correct order 
    result["Region"] = pd.Categorical(result["Region"], categories = regions_dict_according_ccov19kon.values(), ordered=True)
    result["Indikator"] = pd.Categorical(result["Indikator"], categories = indicator_ccov19Regsasong_dict.values(), ordered=True)
    #Drop all .. and .
    result = result[~result.isin(['..']).any(axis=1)]
    result = result[~result.isin(['.']).any(axis=1)]
    #Sort results
    result = result.sort_values(["Region", "Indikator", "Vecka", "År" ]).reset_index(drop=True)
    return(result)

def convert_to_bcov19kom(df: pd.DataFrame):
    """Convert to type of bcov19Kom.csv
    Input:
        df: dataframe of sheet "Veckodata Kommun_stadsdel"
    
    Returns:
        result: dataframe of type bcov19kom.csv
    """
    df = discard_rows(df)
    #Convert some columns to specific type as in the txt file
    df["KnKod"] = df["KnKod"].replace("Okänd", "9999")
    df["KnNamn"] = df["KnNamn"].replace("Malung", "Malung-Sälen")
    df["Kommun"] = df["KnKod"] + " " + df["KnNamn"]

    df["nya_fall_vecka"] = df["nya_fall_vecka"].replace("<15", np.nan)
    #df["nya_fall_vecka"] = df["nya_fall_vecka"].fillna(np.nan)
    df["nya_fall_vecka"] = df["nya_fall_vecka"].astype(float)
    
    #Add 0 if veckonummer is a single number
    df["veckonummer"] = df["veckonummer"].apply(lambda x: ("0" + str(x)) if len(str(x)) == 1 else x)
    #Combine in a new column "År och vecka" the year and week number
    df["År och vecka"] = df["år"].astype(str) + " v " + df["veckonummer"].astype(str)
    #Create new dataframe with specific chosen columns
    df_kommun_stadsdel = df[["Kommun", "År och vecka", "nya_fall_vecka"]]
    #Change column names
    df_kommun_stadsdel.columns = ["Kommun", "År och vecka", "Antal fall"]
    #Print all rows where Kommun is Stockholm
    #Group by Kommun and År och vecka and sum the values
    df_kommun_stadsdel = df_kommun_stadsdel.groupby(["Kommun", "År och vecka"])[["Antal fall"]].sum().reset_index()
    # Melt the DataFrame
    df_melted = pd.melt(df_kommun_stadsdel, id_vars=["Kommun", "År och vecka"], var_name='Indikator', value_name='Fall efter kommun och vecka (tidsserie).')
    
    #Groupby
    result = df_melted.groupby(["Kommun", "Indikator", "År och vecka"])['Fall efter kommun och vecka (tidsserie).'].sum().reset_index()
    return(result)

def convert_to_ccov19reg(df: pd.DataFrame):
    """Convert to type of ccov19reg.csv
    Input:    df: dataframe of sheet "Veckodata Region"
    """
    df = discard_rows(df)
    #Add 0 if veckonummer is a single number
    df["veckonummer"] = df["veckonummer"].apply(lambda x: ("0" + str(x)) if len(str(x)) == 1 else x)
    #Combine in a new column "År och vecka" the year and week number
    df["År och vecka"] = df["år"].astype(str) + " v " + df["veckonummer"].astype(str) 
    #Create new dataframe with specific chosen columns
    df = df[["År och vecka", "Region", "Antal_fall_vecka", "Antal_intensivvårdade_vecka", "Antal_avlidna_vecka"]]
    #Change column names
    df.columns = ["År och vecka","Region", "Antal fall", "Antal intensivvårdade fall", "Antal avlidna"]
    # Melt the DataFrame
    df_melted = pd.melt(df, id_vars=["År och vecka", "Region"], var_name='Indikator', value_name='Bekräftade fall')
    #Groupby
    result = df_melted.groupby(["Region", "Indikator", "År och vecka"])['Bekräftade fall'].sum().reset_index()

    #Change name of files to get the in them same type as in txt file
    result["Region"] = result["Region"].replace("Jämtland Härjedalen", "Jämtland")
    result["Region"] = result["Region"].replace("Sörmland", "Södermanland")
    riket = result.groupby(["Indikator", "År och vecka"])['Bekräftade fall'].sum().reset_index()

    #Add riket to results as a new region in dataframe
    riket["Region"] = "Riket"
    #Combine two dataframes
    result = pd.concat([result, riket], ignore_index=True)
    #Sort in correct order  
    result["Region"] = pd.Categorical(result["Region"], categories = regions_dict_according_ccov19kon.values(), ordered=True)
    result["Indikator"] = pd.Categorical(result["Indikator"], categories = indicator_ccov19Reg_dict.values(), ordered=True)

    #Sort results
    result = result.sort_values(["Region", "Indikator", "År och vecka"]).reset_index(drop=True)
    
    return(result)
