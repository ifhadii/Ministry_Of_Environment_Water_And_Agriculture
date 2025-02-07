from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials
pwd = os.getenv('pwd')
uid = os.getenv('uid')
server = "localhost"
db = "Flour_Customers"
port = "5432"
dir = r'D:\Downolads\Data_Engineering\Ministry_Of_Environment_Water_And_Agriculture\Data_Source'

# Extract function
def extract():
    output = ""
    try:
        for filename in os.listdir(dir):
            file_wo_ext = os.path.splitext(filename)[0]
            if filename.endswith(".xlsx"):
                f = os.path.join(dir, filename)
                if os.path.isfile(f):
                    df = pd.read_excel(f)
                    output += load(df, file_wo_ext) + "\n"
    except Exception as e:
        output += f"Data extract error: {dir} - {str(e)}\n"
    return output

# Load function
def load(df, tbl):
    output = ""
    try:
        engine = create_engine(f'postgresql+psycopg2://{uid}:{pwd}@{server}:{port}/{db}')
        output += f'Importing rows into stg_{tbl}...\n'
        df.to_sql(f"stg_{tbl}", engine, if_exists='replace', index=False)
        output += "Data imported successfully\n"
    except Exception as e:
        output += f"Data load error: {str(e)}\n"
    return output

# Run extraction
try:
    result = extract()
    print(result)
except Exception as e:
    print(f"Error while extracting data: {str(e)}")
