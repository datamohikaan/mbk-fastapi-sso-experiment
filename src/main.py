from fastapi import FastAPI, Depends, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.models import TokenResponse, UserInfo
from src.controller import AuthController

import pandas as pd
import os
import shutil
import psycopg2
import openpyxl
from typing import Union
import json
from datetime import datetime
from rdflib import Graph, URIRef, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, DCTERMS, SKOS, XSD
import uuid
import requests

# Initialize the FastAPI app

app = FastAPI()

# Initialize the HTTPBearer scheme for authentication
bearer_scheme = HTTPBearer()


# Define the root endpoint
@app.get("/")
async def read_root():
    """
    Root endpoint that provides a welcome message and documentation link...
    """
    return AuthController.read_root()

@app.get("/archimate")
def read_archimate():
    """
    archimate endpoint that provides a named graph architectuurmodel
    """
    url = "https://publicaties.belastingdienst.nl/intranet/bcao/architectuurrepository/Kwaliteitsrapportages/modellenbibliotheek.xlsx"
    resp = requests.get(url)

    base_filename = 'modellenbibliotheek.xlsx'
    base_filename_laatste = 'modellenbibliotheek_laatste.xlsx'
    base_filename_voorlaatste = 'modellenbibliotheek_voorlaatste.xlsx'
  
    path_laatste = '/app/Downloads/'+base_filename_laatste 
    path_voorlaatste = '/app/Downloads/'+base_filename_voorlaatste 

    if os.path.isfile( path_laatste ) :
       # Rename the file
       os.rename(path_laatste, path_voorlaatste) 
    else:
        print("File does not exists. Cannot rename")
   
    base_filename = 'modellenbibliotheek_laastste.xlsx'
    timestamp = datetime.now().strftime('%Y%m%d%H%M')
    file_path = f"./Downloads/{base_filename.split('.')[0]}_{timestamp}.{base_filename.split('.')[-1]}"
    output = open(file_path, 'wb')
    output.write(resp.content)
    output.close()
    # use copyfile() 
    shutil.copy(file_path, path_laatste) 
     
    if os.path.getsize(path_laatste) == os.path.getsize(path_voorlaatste):    
          print ("The files laatste en voorlaatste have the same size, nothing to do here.")    
          return ("The files laatste en voorlaatste have the same size, nothing to do here ->https://publicaties.belastingdienst.nl/intranet/bcao/architectuurrepository/Kwaliteitsrapportages/modellenbibliotheek.xlsx")    
    else:   
          print("The files laatste en voorlaatste have different sizes")
     
    ######################################################################################################################
    # GENERAL #
    ######################################################################################################################

    g = Graph()
    NS_AM = Namespace("http://modellenbibliotheek.belastingdienst.nl/def/amm#")
    g.bind("amm", NS_AM)
    g.bind("skos", SKOS)
 
    if resp.status_code == 200:
        
        #excel_file = base_filename
        excel_file = file_path

        def create_uri(name: str) -> URIRef:
           """Each cononical uri has the following structure according to RFC4122.
           :param name: name of the namespace
           :return: the created uri
           """
           #uuidstr = uuid.uuid5(uuid.NAMESPACE_URL, name)
           uuidstr = name
           return URIRef(f"urn:uuid:{uuidstr}")
 
        # Read the Excel-file
        try:
            df0 = pd.read_excel(excel_file )
            df = df0.dropna(subset=['uniek ID']) # verwijder lege unieke ID
            print("reading Excel file was okay")
        except:
            print("Error reading the god damn Excel file")
   
        sjebang = 1
        for index, row in df.iterrows():
            # print(index)
            if sjebang == 1  :
            #if index == 0:
   
                if "-ID"  in row['ID van objecten']:
                   if len(row['uniek ID']) == 36 :  # soms staat er een tekst... 
                        subject = create_uri(row['uniek ID'])
                        print (subject)
                        a  = row['ID van objecten']
                        a = a.replace("-ID", "")
                        a = a.capitalize()
                        print (a)
                        g.add ((subject, RDF.type , Literal(NS_AM + a ) ))
                        g.add((subject, RDFS.label, Literal(row['naam'])))
                        g.add((subject, RDFS.comment, Literal(row['documentatie'])))
                        g.add((subject, NS_AM.besguid, Literal(row['uniek ID'])))
                else:
                     is_id = False 
                     #print ("-ID komt niet voor in de naam")
                     # in derijen daaronder zijn 4 aggregatierelaties van dezelfde administratie naar 4 verschillende bedrijfsobjecten. 
                     if len(row['naam']) == 36 :
                         subject = create_uri(row['naam'])
                     if len(row['uniek ID']) == 36 :
                        obj = create_uri(row['uniek ID'])
                        g.add((subject, NS_AM.bevat, obj ))
                
        file_dir = "/app/"
        file_name_turtle = "turtle.ttl"
                    
        # 🐍🐢
        g.serialize(destination=file_dir + file_name_turtle)
        rdfdata = open(file_dir + file_name_turtle).read()
        #print(rdfdata[:10])
        headers = {"Content-Type": "text/turtle;charset=utf-8"}
        DATASTORE_ENDPOINT = "http://mbk-fuseki:3030/modellenbibliotheek"
        endpoint = DATASTORE_ENDPOINT + "?graph=" + "urn:name:architectuurmodel"
        r = requests.put( endpoint, data=rdfdata.encode("utf-8"), headers=headers, verify=False )
        # Print a message
        print('Import successfully completed!  hope the turtlemania run was okay. ')  
        # return  a message
        return ('hello archimate nice to see you again! hope the turtlemania run was okay  ')
    else:
        return ('Failed to process the arch file') 

 

@app.get("/xls2pg")
def read_xls2pg():
    # https://plainenglish.io/blog/importing-excel-into-a-postgresql-database-with-python
    # curl -X 'GET' 'https://mbiebapi-ont.belastingdienst.nl/xls' -H 'accept: application/json'
    # curl -i -k -X GET -H 'Content-Type: application/json'  https://publicaties.belastingdienst.nl/intranet/bcao/architectuurrepository/Kwaliteitsrapportages/modellenbibliotheek.xlsx
    # curl https://publicaties.belastingdienst.nl/intranet/bcao/architectuurrepository/Kwaliteitsrapportages/modellenbibliotheek.xlsx"
    # curl -i -k -X GET -H 'Content-Type: application/json'  https://mbiebapi-ont.belastingdienst.nl/xlscurl https://publicaties.belastingdienst.nl/intranet/bcao/architectuurrepository/Kwaliteitsrapportages/modellenbibliotheek.xlsx --output hoi.xlsx
    # resp = requests.post(my_endpoint_var, headers=header_var, data=post_data_var)
    url = "https://publicaties.belastingdienst.nl/intranet/bcao/architectuurrepository/Kwaliteitsrapportages/modellenbibliotheek.xlsx"
    resp = requests.get(url)
    if resp.status_code == 200:
        # with open(file_Path, 'wb') as file:
        #    file.write(response.content)
        base_filename = 'modellenbibliotheek.xlsx'
        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_path = f"./Downloads/{base_filename.split('.')[0]}_{timestamp}.{base_filename.split('.')[-1]}"
        # output = open('./Downloads/modellenbibliotheek.xls', 'wb')
        output = open(file_path, 'wb')
        output.write(resp.content)
        output.close()
        # Save Excel file location into a variable
        ##excel_data_df = pd.read_excel("/Users/boscp08/src/Projects/github-cloud/mbk-fastapi/Downloads/modellenbibliotheek.xlsx", sheet_name='modellenbibliotheek',usecols=['ID van objecten', 'naam', 'uniek ID', 'documentatie'])
        excel_file = file_path
        #excel_file = base_filename
        
        # Open the Excel workbook and load the active sheet into a variable
        workbook = openpyxl.load_workbook(excel_file)
        sheet = workbook.active
        #https://www.youtube.com/watch?v=ZoR3AfIUB0k
        sheet.delete_cols(5) #the one is none
        #sheet.delete_cols(4) #documentatie eraf
        
        
        # Create a list with the column names in the first row of the workbook
        column_names = [column.value for column in sheet[1]]
        # Create an empty list
        data = []
        # Iterate over the rows and append the data to the list
        for row in sheet.iter_rows(min_row=2, values_only=True):
            data.append(row)

        #connection = psycopg2.connect(
        #    database='database1',
        #    user='user1',
        #    password='password1',
        #    host='mbk-postgres',
        #    port='5432'
        #)
        
        connection = psycopg2.connect(
        database='app',
        user='app',
        password='dmV6JVIDQKJu307JhaSGlL3z3QGqelTYzeQS2oxNIOO8anwE9fFyZnfqeNXEgKs1',
        #host='cluster-mbk-rw',
        host='mbk-postgres',
        port='5432'
        )
        
        connection.set_client_encoding('UTF8')
        # Open a cursor object to perform database operations
        cursor = connection.cursor()
        # Set a name for the PostgreSQL schema and table where we will put the data
        schema_name = 'public'
        table_name = 'ibes'
        table_drop_query = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name};
        """
        table_truncate_query = f"""
        TRUNCATE TABLE {schema_name}.{table_name};
        """
        # Write a query to create a table in the schema. It must contain all
        # columns in column_names
        table_creation_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {", ".join([f'"{name}" TEXT' for name in column_names])}
        )
        """
        table_insert_query = f"""
        INSERT INTO  {schema_name}.{table_name} VALUES('A', 'B', 'C', 'D');
        """
        #INSERT INTO  {schema_name}.{table_name} VALUES('A', 'B', 'C', 'D', null );

        # Create a parameterized SQL query to insert the data into the table
        insert_data_query = f"""
        INSERT INTO {schema_name}.{table_name} ({", ".join([f'"{name}"' for name in column_names])})
        VALUES ({", ".join(['%s' for _ in column_names])})
        """
        # Execute the query using the data list as parameter
        # Use the cursor to execute both queries
        #cursor.execute(table_drop_query)
        cursor.execute(table_creation_query)
        #database1=# \d ibes
        #                   Table "public.ibes"
        #     Column      | Type | Collation | Nullable | Default
        #-----------------+------+-----------+----------+---------
        # ID van objecten | text |           |          |
        # naam            | text |           |          |
        # uniek ID        | text |           |          |
        # documentatie    | text |           |          |

        #-- Table: public.ibes
        #-- DROP TABLE IF EXISTS public.ibes;

        #CREATE TABLE IF NOT EXISTS public.ibes
        #(
        # "ID van objecten" text COLLATE pg_catalog."default",
        # naam text COLLATE pg_catalog."default",
        # "uniek ID" text COLLATE pg_catalog."default",
        # documentatie text COLLATE pg_catalog."default"
        #)
        #  TABLESPACE pg_default;
        # ALTER TABLE IF EXISTS public.ibes
        # OWNER to app;

        cursor.execute(table_truncate_query)
        connection.commit()
        #cursor.execute(table_insert_query)
        cursor.executemany(insert_data_query, data)
        # don 't forget committng the changes to the database persistent
        connection.commit()
        # Close communication with the database
        cursor.close()
        connection.close()
        # return  a message
        return ('table created ....execute many ? File downloaded and imported successfully completed!')
    else:
        return ('Failed to download file')


# Define the login endpoint
@app.post("/login", response_model=TokenResponse)
async def login(username: str = Form(...), password: str = Form(...)):
    """
    Login endpoint to authenticate the user and return an access token.

    Args:
        username (str): The username of the user attempting to log in.
        password (str): The password of the user.

    Returns:
        TokenResponse: Contains the access token upon successful authentication.
    """
    return AuthController.login(username, password)


# Define the protected endpoint
@app.get("/protected", response_model=UserInfo)
async def protected_endpoint(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    """
    Protected endpoint that requires a valid token for access.

    Args:
        credentials (HTTPAuthorizationCredentials): Bearer token provided via HTTP Authorization header.

    Returns:
        UserInfo: Information about the authenticated user.
    """
    return AuthController.protected_endpoint(credentials)
