from defined import *
import pandas as pd
from datetime import date

def acov19dag(response_json: str):
    """Format the data from the response according to acov19DAG file and save it as a csv file
    
    Input:  response_json: the response from the request
    """

    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, dag = item_in_response["key"]
        region = regions_dict_acov19dag[region_code]
        value = int(item_in_response["values"][0])
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            'Dag': dag,
            'Fall per dag': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], regions_dict_acov19dag.values(), ordered=True)

    # Save the DataFrame as a csv file
    df.to_csv("data/"+ date.today().strftime("%Y%m%d") +"/acov19DAG.csv", sep=',', index=False)

def bcov19Kom(response_json: str, response_json_2: str):
    """Format the data from the response according to bcov19Kom file and save it as a csv file
    Input:  response_json: the response from the request
            response_json_2: the response from the request
    """
    formatted_data = []
    for item_in_response in response_json["data"]:
        kommun_code, indicator_code, date_week_year = item_in_response['key']
        kommun = kommun_dict[kommun_code]
        indicator = indicator_bcov19kom_dict[indicator_code]
        if indicator in categories_to_skip:
            continue
        date_week_year = str(date_week_year[:4]) + " v "+ str(date_week_year[5: ])
        #value = float(item_in_response['values'][0]) if item_in_response['values'][0] != '..' else ".."  # Convert '.' to None for missing values
        value = item_in_response['values'][0]

        formatted_data.append({
            'Kommun': kommun,
            'Indikator': indicator,
            'År och vecka': date_week_year,
            'Fall efter kommun och vecka (tidsserie).': value
        })

    df1 = pd.DataFrame(formatted_data)

    formatted_data = []
    for item_in_response in response_json_2["data"]:
        kommun_code, indicator_code, date_week_year = item_in_response['key']
        kommun = kommun_dict[kommun_code]
        indicator = indicator_bcov19kom_dict[indicator_code]
        if indicator in categories_to_skip:
            continue
        date_week_year = str(date_week_year[:4]) + " v "+ str(date_week_year[5: ])
        #value = float(item_in_response['values'][0]) if item_in_response['values'][0] != '..' else ".."  # Convert '.' to None for missing values
        value = item_in_response['values'][0]

        formatted_data.append({
            'Kommun': kommun,
            'Indikator': indicator,
            'År och vecka': date_week_year,
            'Fall efter kommun och vecka (tidsserie).': value
        })

    df2 = pd.DataFrame(formatted_data)
    #Merge two dataframes
    df = pd.concat([df1, df2], ignore_index=True)
    
    df = df.sort_values(['Kommun', 'Indikator', 'År och vecka'], ascending = [True, True, True])
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/bcov19Kom.csv", sep=',', index=False)


def ccov19kon(response_json: str):
    """Format the data from the response according to ccov19Kon file and save it as a csv file
    
    Input:      response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, indicator, gender, date_year  = item_in_response["key"]
        region = regions_dict_according_ccov19kon[region_code]
        indicator_ccov19kon = indicator_ccov19kon_dict[indicator]
        if indicator_ccov19kon in categories_to_skip:
            continue
        gender_ccov19kon = gender_dict[gender]
        date_year = str(date_year[:4]) + " v "+ str(date_year[5: ])
        value = item_in_response["values"][0]
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            "Indikator": indicator_ccov19kon,
            "Kön": gender_ccov19kon,
            'År och vecka': date_year,
            'Fall efter kön, region och vecka (tidsserie).': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], categories = regions_dict_according_ccov19kon.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ccov19Kon.csv", sep=',', index=False)


def ccov19Reg(response_json: str):
    """Format the data from the response according to ccov19Reg file and save it as a csv file
    
    Input:  response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, indicator, date_week_year  = item_in_response["key"]
        region = regions_dict_acov19dag[region_code]
        indicator_ccov19Reg = indicator_ccov19Reg_dict[indicator]
        if indicator_ccov19Reg in categories_to_skip:
            continue
        date_week_year = str(date_week_year[:4]) + " v "+ str(date_week_year[5: ])
        value = item_in_response["values"][0]
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            "Indikator": indicator_ccov19Reg,
            'År och vecka': date_week_year,
            'Bekräftade fall': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], categories = regions_dict_acov19dag.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ccov19Reg.csv", sep=',', index=False)

def ccov19Regsasong(response_json: str):
    """Format the data from the response according to ccov19Regsasong file and save it as a csv file
    
    Input:      response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, indicator, week, year  = item_in_response["key"]
        region = regions_dict_acov19dag[region_code]
        indicator_ccov19Regsasong = indicator_ccov19Regsasong_dict[indicator]
        if indicator_ccov19Regsasong in categories_to_skip:
            continue
        value = item_in_response["values"][0]
        week =  "v " + str(week[1:])
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            "Indikator": indicator_ccov19Regsasong,
            'Vecka': week,
            "År": year,
            'Fall, intensivvårdade och avlidna efter region och vecka (säsongsvis).': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], categories = regions_dict_acov19dag.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ccov19Regsasong.csv", sep=',', index=False)

def dcov19ald(response_json: str):
    """Format the data from the response according to dcov19ald file and save it as a csv file

    Input:    response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        indicator, age_group, date_week_year  = item_in_response["key"]
        indicator_dcov19ald = indicator_ccov19kon_dict[indicator]
        if indicator_dcov19ald in categories_to_skip:
            continue
        value = item_in_response["values"][0]
        if age_group == "Saknas":
            age_group = "Uppgift saknas"
        date_week_year = str(date_week_year[:4]) + " v "+ str(date_week_year[5: ])
        # Append the extracted data to the list
        formatted_data.append({
            "Indikator": indicator_dcov19ald,
            'Åldersgrupp': age_group,
            "År och vecka": date_week_year,
            'Fall efter åldersgrupp, vecka och år.': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df = df.sort_values(['Indikator', 'Åldersgrupp', 'År och vecka'], ascending = [True, True, True])
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/dcov19ald.csv", sep=',', index=False)

def ecov19sabo(response_json: str):
    """Format the data from the response according to ecov19sabo file and save it as a csv file

    Input:      response_json: the response from the request
    """
        # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, category, date_year  = item_in_response["key"]
        region = regions_dict_acov19dag[region_code]
        category_ecov19sabo = category_ecov19sabo_dict[category]
        if category_ecov19sabo in categories_to_skip:
            continue
        date_year = str(date_year[:4]) + " v "+ str(date_year[5: ])
        value = item_in_response["values"][0]
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            "Kategori": category_ecov19sabo,
            'År och vecka': date_year,
            'Fall bland personer 65 år och äldre med insats enligt socialtjänstlagen efter region och vecka (tidsserie).': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], categories = regions_dict_acov19dag.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ecov19sabo.csv", sep=',', index=False)


def ecov19sabosasong(response_json: str):
    """Format the data from the response according to ecov19sabosasong file and save it as a csv file

    Input    response_json: the response from the request
    """
        # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, category, week, year = item_in_response["key"]
        category_ecov19sabo = category_ecov19sabo_dict[category]
        region = regions_dict_acov19dag[region_code]
        if category_ecov19sabo in categories_to_skip:
            continue
        week =  "v " + str(week[1:])
        value = item_in_response["values"][0]
        if value == ".." or value == ".":
            continue
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            "Kategori": category_ecov19sabo,
            'Vecka': week,
            "År": year,
            'Fall bland personer 65 år och äldre med insats enligt socialtjänstlagen': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], categories = regions_dict_acov19dag.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ecov19sabosasong.csv", sep=',', index=False)

def PCRtestVAr(response_json: str, suffix: str):
    """Format the data from the response according to PCRtestVAr file and save it as a csv file
    Depending on the suffix the data will be saved as PCRtestVAr_k.csv, PCRtestVAr_m.csv or PCRtestVAr_s.csv
    The suffix correspond to if it should focus on k = Kvinnor, m  = Män, s = Uppgift om kön saknas.

    Input       response_json: the response from the request
                suffix: the suffix of the file
    """
        # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code, category, age_group_key,_ , date_year  = item_in_response["key"]
        region = regions_dict_PCRtestVAr[region_code]
        indicator_PCRtestVAr = indicator_PCRtestVAr_dict[category]
        if indicator_PCRtestVAr in categories_to_skip:
            continue
        age_group = age_group_dict[age_group_key]
        date_year = str(date_year[:4]) + " v "+ str(date_year[5: ])
        value = item_in_response["values"][0]
        # Append the extracted data to the list
        formatted_data.append({
            'Region': region,
            "Kategori": indicator_PCRtestVAr,
            "Åldersgrupp": age_group,
            "Kön": gender_suffix_dict[suffix],
            'År och vecka': date_year,
            'Testade individer med PCR': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    df["Region"] = pd.Categorical(df["Region"], categories = regions_dict_PCRtestVAr.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/PCRtestVAr_"+suffix+".csv", sep=',', index=False)

def xcov19ivavDAG(response_json: str):
    """Format the data from the response according to xcov19ivavDAG file and save it as a csv file

    Input:    response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        indicator, day  = item_in_response["key"]
        indicator_xcov19ivavDAG = indicator_xcov19ivavDAG_dict[indicator]
        if indicator_xcov19ivavDAG in categories_to_skip:
            continue
        value = item_in_response["values"][0]
        #Case when the day is given value 2020-01-01. Change it to "Okänd tidpunkt"
        if day == "2020-01-01":
            day = "Okänd tidpunkt"
        # Append the extracted data to the list
        formatted_data.append({
            "Indikator": indicator_xcov19ivavDAG,
            "Dag": day,
            'Intensivvårdade respektive avlidna per dag': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    #df = df.sort_values(['Indikator', 'Dag'], ascending = [True, True])
    df["Indikator"] = pd.Categorical(df["Indikator"], categories = indicator_xcov19ivavDAG_dict.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/xcov19ivavDAG.csv", sep=',', index=False)


def ycov19ivavald(response_json: str):
    """Format the data from the response according to ycov19ivavald file and save it as a csv file

    Input:    response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        indicator, age_group, date_year  = item_in_response["key"]
        indicator_ycov19ivavald = indicator_ycov19ivavald_dict[indicator]
        if indicator_ycov19ivavald in categories_to_skip:
            continue
        value = item_in_response["values"][0]
        #if age_group == "Saknas":
        #    age_group = "Uppgift saknas"
        age_group = age_group_dict_ycov19ivavald[age_group]
        date_year = str(date_year[:4]) + " v "+ str(date_year[5: ])
        # Append the extracted data to the list
        formatted_data.append({
            "Indikator": indicator_ycov19ivavald,
            "Åldersgrupp": age_group,
            "År och vecka": date_year,
            'Intensivvårdade och avlidna fall efter åldersgrupp och vecka (tidsserie).': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    #df = df.sort_values(['Indikator', 'Dag'], ascending = [True, True])
    df["Indikator"] = pd.Categorical(df["Indikator"], categories = indicator_ycov19ivavald_dict.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ycov19ivavald.csv", sep=',', index=False)


def ycov19ivavkon(response_json: str):
    """Format the data from the response according to ycov19ivavkon file and save it as a csv file

    Input    response_json: the response from the request
    """
    # Extract data for the DataFrame
    formatted_data = []
    for item_in_response in response_json["data"]:
        indicator, gender, date_year  = item_in_response["key"]
        indicator_ycov19ivavkov = indicator_ycov19ivavkov_dict[indicator]
        if indicator_ycov19ivavkov in categories_to_skip:
            continue
        value = item_in_response["values"][0]
        #if age_group == "Saknas":
        #    age_group = "Uppgift saknas"
        gender = gender_dict[gender]
        date_year = str(date_year[:4]) + " v "+ str(date_year[5: ])
        # Append the extracted data to the list
        formatted_data.append({
            "Indikator": indicator_ycov19ivavkov,
            "Kön": gender,
            "År och vecka": date_year,
            'Intensivvårdade och avlidna fall efter kön och vecka (tidsserie).': value
        })
    # Create the DataFrame
    df = pd.DataFrame(formatted_data)
    # Sort the DataFrame according to the already given data
    #df = df.sort_values(['Indikator', 'Dag'], ascending = [True, True])
    df["Indikator"] = pd.Categorical(df["Indikator"], categories = indicator_ycov19ivavkov_dict.values(), ordered=True)
    # Save the DataFrame as a csv file
    df.to_csv("data/"+date.today().strftime("%Y%m%d")+"/ycov19ivavkon.csv", sep=',', index=False)

def format_scb(response_json: str):
    """Format the data from the response according to the SCB file and save it as a csv file
    Input:  response_json: the response from the request
    """
    formatted_data = []
    for item_in_response in response_json["data"]:
        region_code = item_in_response['key'][0]
        #Skip the 4 last characters
        region = scb_municipalities[region_code][:-4]
        if region[-1] == "s":
            region = region[:-1]
        value = item_in_response['values'][0]
        
        formatted_data.append({
            'Region': region,
            'Inhabitants': int(value)
        })
    df = pd.DataFrame(formatted_data)
    df.to_csv("Municipalities_2022.csv", sep=',', index=False)