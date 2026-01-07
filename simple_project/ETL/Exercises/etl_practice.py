import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

def extract_from_csv(input_file):
    dataframe = pd.read_csv(input_file)
    return dataframe

def extract_from_json(input_file):
    dataframe = pd.read_json(input_file, lines=True)
    return dataframe

def extract_from_xml(input_file):
    tree = ET.parse(input_file)
    root = tree.getroot()
    xml_columns = ['car_model', 'year_of_manufacture', 'price', 'fuel']
    dataframe = pd.DataFrame(columns=xml_columns)

    for item in root:
        car_model = item.find("car_model").text
        year_of_manufacture = item.find("year_of_manufacture").text
        price = float(item.find("price").text)
        fuel = item.find("fuel").text

        row_df = pd.DataFrame([[car_model, year_of_manufacture, price, fuel]], columns=xml_columns)
        dataframe = pd.concat([dataframe, row_df], ignore_index=True)
        return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    for csvfile in glob.glob("*.csv"): 
        if csvfile != target_file:  # check if the file is not the target file
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
        
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
    
    for xmlfile in glob.glob("*.xml"): 
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
    
    return extracted_data 

def Transform_extracted_data(input_data):
    input_data['price'] = round(input_data.price, 2)

    return input_data

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

# Log the initialization of the ETL process 
log_progress("etl_practice.py ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = Transform_extracted_data(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("etl_practice.py ETL Job Ended") 
