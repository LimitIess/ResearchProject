from transformations import *
from format_data import *
import json
import os
import requests
from defined import *
from datetime import date
import time

def make_dir():
    """ 
    Create a folder to store the data in if it does not exist
    """
    day = date.today().strftime("%Y%m%d")
    #Create empty folder to store data
    if not os.path.exists("data"):
        os.mkdir("data")
    
    #Create date folder
    if not os.path.exists("data/" + day):
        os.mkdir("data/" + day)

def collect_data_from_API():
    """ Collect data from the API and save it as a csv file """    
    make_dir()
    #Create session
    session = requests.Session()
    
    filenames = {"acov19DAG": acov19dag,"ccov19kon":ccov19kon,"ccov19Reg":ccov19Reg,"ccov19Regsasong":ccov19Regsasong,"dcov19ald":dcov19ald,"ecov19sabo":ecov19sabo,"ecov19sabosasong":ecov19sabosasong,"xcov19ivavDAG":xcov19ivavDAG,"ycov19ivavald":ycov19ivavald,"ycov19ivavkon":ycov19ivavkon}
    filenames_pcr =  {"k":json_question_PCR_k,"m":json_question_PCR_m,"s":json_question_PCR_s}
    #Send the request
    for name_of_file, parsing_function in filenames.items():
        time.sleep(5)
        url = f"http://fohm-app.folkhalsomyndigheten.se/Folkhalsodata/api/v1/sv/A_Folkhalsodata/H_Sminet/covid19/falldata/{name_of_file}.px"
        #Get the response
        response = session.post(url, json=json_question)
        #If the response was successful, no Exception will be raised
        if response.status_code == 200:
            response_json = json.loads(response.content.decode('utf-8'))
            parsing_function(response_json)
            #Stop the code for 2 seconds to not overload the server
            time.sleep(2)
            #Format data
            acov19dag(response_json)
            bcov19kom(response_json)
            ccov19Kon(response_json)
            ccov19Reg(response_json)
            ccov19Regsasong(response_json)
            dcov19ald(response_json)
            ecov19sabo(response_json)
            ecov19sabosasong(response_json)
            PCRtestVAr(response_json, "k")
            PCRtestVAr(response_json, "m")
            PCRtestVAr(response_json, "s")
            xcov19ivavDAG(response_json)
            ycov19ivavald(response_json)
            ycov19ivavkon(response_json)
            print(f"Finished {name_of_file}")
        else:
            print("Response was not successful")

    #For PCRtestVAr case
    url_pcr = "http://fohm-app.folkhalsomyndigheten.se/Folkhalsodata/api/v1/sv/A_Folkhalsodata/H_Sminet/covid19/testdata/PCRtestVAr.px"
    for name_of_file, pcr_json_question in filenames_pcr.items():
        time.sleep(5)
        #May need to chance the json_question
        response = session.post(url_pcr, json=pcr_json_question)
        #If the response was successful, no Exception will be raised
        if response.status_code == 200:
            response_json = json.loads(response.content.decode('utf-8'))
            PCRtestVAr(response_json, name_of_file)
            print(f"Finished PCRtestVAr_{name_of_file}")
        else:
            print("Response was not successful")
    
    #For bcov19Kom case
    url = "http://fohm-app.folkhalsomyndigheten.se/Folkhalsodata/api/v1/sv/A_Folkhalsodata/H_Sminet/covid19/falldata/bcov19Kom.px"
    time.sleep(5)
    #Get the response
    response = session.post(url, json=json_question_bcov19kom_1)
    #If the response was successful, no Exception will be raised
    if response.status_code == 200:
        response_json_1 = json.loads(response.content.decode('utf-8'))
        time.sleep(5)
        response = session.post(url, json=json_question_bcov19kom_2)
        if response.status_code == 200:
            response_json_2 = json.loads(response.content.decode('utf-8'))
            bcov19Kom(response_json_1, response_json_2)
            print(f"Finished bcov19Kom")
        else:
            print("Response was not successful")
    else:  
        print("Response was not successful")


def collect_inhabitant_per_municipality():
    """
    Collect data about number of inhabitants from SCB API and save it as a csv file
    """
    #Create session
    session = requests.Session()
    #Send the request
    response = session.post("https://api.scb.se/OV0104/v1/doris/sv/ssd/START/BE/BE0101/BE0101A/BefolkningNy", json=json_question_scb)
    #If the response was successful, no Exception will be raised
    if response.status_code == 200:
        response_json = json.loads(response.content.decode('utf-8'))
        print("Inhabitant data collected")
        format_scb(response_json)
    else:
        print("Response for inhabitant data was not successful")