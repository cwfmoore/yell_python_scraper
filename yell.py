# Import libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import re
import json
import os
import glob
from time import sleep as sleep
from datetime import datetime as dt

class Scraper:

    def __init__(self, settings):

        # Set headers from the text file in current directory
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}

        # Town search settings
        self.ts_enabled = settings.get('enabled')
        self.top_towns = settings.get('top_towns')
        self.specific_towns = settings.get('specific_towns')
        self.n_towns = settings.get('n_towns')
        self.town_names = settings.get('town_names')
        self.ts_search_word = settings.get('ts_search_word')
        self.ts_radius = settings.get('ts_radius')
        self.ts_keywords = settings.get('keywords')
        self.attempts = settings.get('attempts')
        self.wait = settings.get('wait')

    # Function to retrieve list of towns in UK by population
    def getTowns(self):

        # Set the url for the web page
        url = 'https://www.thegeographist.com/uk-cities-population-1000/'

        # Retrieve the table from the webpage
        response = requests.get(url, headers=self.headers)
        soup = bs(response.text, 'lxml')
        table = soup.find('table')

        # Convert to a DataFrame
        table = pd.read_html(str(table))
        df = table[0]
        df = df[['City/Town', 'Population']]
        if isinstance(df, pd.Series): pd.DataFrame(df)

        # Drop towns with '/' in title
        for index, row in df.iterrows():
            if '/' in str(df.loc[index, 'City/Town']):
                df = df.drop(index, axis=0)

        # Iterate through towns
        if self.top_towns:
            df = df.head(self.n_towns)
        elif self.specific_towns:
            for index, row in df.iterrows():
                town = row['City/Town']
                if town not in self.town_names:
                    df = df.drop(index, axis=0)
        return df

    # This function will return a reponse from a specific page number
    def tsRequest(self, page_number, town):

        # Define the search parameters
        params = (
            ('filterDistance', f'{self.ts_radius}'),
            ('keywords', f'{self.ts_search_word}'),
            ('location', f'{town}'),
            ('pageNum', f'{page_number}'),
        )

        response = requests.get('https://www.yell.com/ucs/UcsSearchAction.do', headers=self.headers, params=params)
        return response

    # Convert the response from Request into a pandas DataFrame
    def process(self, response):

        def getBusinessName(article):
            business_name = article.find('span', attrs={'itemprop': 'name'})
            if business_name is not None:
                business_name = business_name.text
            return business_name

        def getAddress(article):
            address = article.find('span', attrs={'itemprop': 'address'})
            if address is not None:
                address = address.text.replace('\n', '')
            return address

        def getTelephone(article):
            telephone = article.find('span', attrs={'itemprop': 'telephone'})
            if telephone is not None:
                telephone = telephone.text.replace(' ', '')
            return telephone

        def getWebsite(article):
            website = [a['href'] for a in article.find_all('a', href=True) if a['href'].startswith('http')]
            if len(website) > 0: website = website[0]
            else: website = None
            return website

        def getListingURL(article):
            links = [a['href'] for a in article.find_all('a', href=True) if a['href'].startswith('/biz')]
            links = list(dict.fromkeys(links))
            links = [f'https://www.yell.com{link}' for link in links if re.search(r'view=map', link) is None]
            if links: links = links[0]
            return links

        soup = bs(response.text, 'lxml')

        articles = [a for a in soup.find_all('article')]

        company_dicts = []

        for article in articles:

            company_dict = {
                'business_name': getBusinessName(article),
                'address': getAddress(article),
                'telephone': getTelephone(article),
                'website': getWebsite(article),
                'listing_url': getListingURL(article)
            }

            company_dicts.append(company_dict)

        df = pd.DataFrame(company_dicts)

        return df

    # Return the current time
    def getNow(self):
        now = dt.now().strftime("%H:%M:%S")
        return now

    # Search the listing page for the company for specific words
    def addKeywords(self):

        # Function to call server for response, designed to catch errors from request.get()
        def request(self, listing_url):

            # Set attempts and maximum attempts
            attempts = 1
            max_attempts = self.attempts

            # Start a loop which will break on successful response
            while True:

                # Request the response from the server
                try:
                    response = requests.get(listing_url, headers=self.headers)
                    break
                except:
                    # If there is an error getting data return None
                    if attempts >= max_attempts:
                        print(f'>>> time: {self.getNow()}, message: "ERROR! Response failed, attempts: {attempts}/{max_attempts}"')

                        response = None
                        break
                    else:
                        attempts += 1
                return response

        # Create a lambda function for printing an update to the user
        print_update = lambda a, b, c, d, e: print(f'>>> time: {self.getNow()}, response: {a}, town: {b}, row_index: {c}/{d}, business_name: {e}')
        
        # This function will create a new csv file in the town folder with keywords added
        # Iterate through the town folders gathering csvs matching searches
        directories = glob.glob(f'{os.getcwd()}/data/*/')
        for directory in directories:

            # Parse out the town name from the directory path
            town = directory.replace('\\', '/').split('/')[-2]

            # Get the paths of all the csv files in the directory matching search word
            csvs = glob.glob(f'{directory}{self.formatSearchWord()}.csv')

            # Check to see if there is already a csv file for the keywords
            csvs_kw = glob.glob(f'{directory}{self.formatSearchWord()}_kw.csv')

            # If keywords csv file is present skip to next directory
            if len(csvs_kw) == 0:

                # Iterate through the csvs that match the search word
                for csv in csvs:
                    
                    # Convert the csv into a DataFrame
                    df = pd.read_csv(csv, index_col='index', dtype={'telephone': str})

                    # Iterate through the urls and get the keywords
                    for index, row in df.iterrows():

                        # Start a loop that will break on a sucessful response
                        while True:

                            # Set attempts and max attempts
                            attempts = 1
                            max_attemps = self.attempts
                            
                            # Get the url for the listing on yell.com
                            listing_url = row.listing_url

                            # Request the response from the server
                            response = requests.get(listing_url, headers=self.headers)
                                        
                            # Conditional to check that response ok from server
                            if response.ok:

                                # Grab the sectioons of the page containing company infromation
                                soup = bs(response.content, 'lxml')
                                soup = [str(b) for b in soup.find_all('div', attrs={'class': 'grid grid-fluid'})]
                                soup = ''.join(soup)

                                # Print a message in the terminal window to update the user
                                print_update(response.status_code, town, (index + 1), len(df), row.business_name)

                                # Iterate over the keywords, updating a boolean value if founf in .text
                                for keyword in self.ts_keywords:
                                    if re.search(keyword, soup) is not None:
                                        df.loc[index, keyword] = True
                                    else: df.loc[index, keyword] = False
                                
                                # Break the while loop now that DataFrame was updated
                                break
                            
                            # Code to run if the response is not ok
                            else:
                                message = '"Server not responding, trying again in {self.wait} seconds"'
                                print(f'>>> time: {self.getNow()}, response: {reponse.status_code}, attempts: {attempts}/{max_attempts}, message: {message}')
                                attempts += 1
                                sleep(self.wait)

                    # Save a new csv file for keywords and print message to user
                    print(f'>>> time: {self.getNow()}, message: "Saving csv for {town}"')
                    df.to_csv(f'{directory}{self.formatSearchWord()}_kw.csv')
            
            # Update user that the file already exist and moving to next directory
            else: print(f'>>> time: {self.getNow()}, message: "Keywords csv file already present in {town} directory, moving to next town"')

    # Change format of search terms to accomodate file paths
    def formatSearchWord(self):

        # For the purposes of folder path strings
        search_word = self.ts_search_word.lower().replace(' ', '_')
        return search_word

    # Create a new folder for each town to save search results
    def setTownFolders(self, towns):

        # Set a data folder if it does not exist
        data_folder = f'{os.getcwd()}/data'
        if not os.path.isdir(data_folder): os.mkdir(data_folder)

        # Iterate through towns creating folders
        for town in towns:
            town_dir = f'{os.getcwd()}/data/{town}'
            if not os.path.isdir(town_dir): os.mkdir(town_dir)

    # The searchTowns function searches towns for results and stores csv files in folder
    def searchTowns(self):

        # Settings for town searcb
        enabled = self.ts_enabled
        top_towns = self.top_towns
        n_towns = self.n_towns

        # Load towns DataFrame and select relevant towns
        df_towns = self.getTowns()

        # Get list of selected towns
        towns = list(df_towns['City/Town'])

        # Set the folders for each towns data
        self.setTownFolders(towns)

        # Iterate though towns saving data to own csv file
        for town in towns:

            # Update the user on the change of town search
            print(f'>>> time: {self.getNow()}, message: "Searching {town} for {self.ts_search_word}"')

            # Set attempts and max_attemps
            attempts = 1
            max_attempts = self.attempts

            # Set starting page number
            page_number = 1

            # Create an empty list for holding new DataFrames
            dfs = []

            # Create an empty list to hold the number of records
            total_records = []

            # Function to update the user of search progress
            print_update = lambda u, v, w, x, y, z: print(f'>>> time: {self.getNow()}, attempt: {attempts}/{max_attempts}, page_number: {v}, records: {w}, total_records: {x}, response_code: {y}, message: "{z}"')

 
            # Start a loop to increment through search pages
            while True:

                # Request data from the server
                response = self.tsRequest(page_number, town)

                # Process search results into a DataFrame 
                df = self.process(response)

                # Condition to check status code and numbwe of records returned
                if response.ok and len(df) != 0:

                    # Successful loop add DataFrame to the dfs list and record total records
                    dfs.append(df)
                    total_records.append(len(df))
                    message = 'SUCCESS!'
                    print_update(attempts, page_number, len(df), sum(total_records), response.status_code, message)
                    page_number += 1
                
                elif response.ok and len(df) == 0:

                    # DataFrame maybe empty as server is restricting traffic, wait and try again
                    message = f'Server response ok but no records retrieved. Waiting {self.wait} seconds before trying again'
                    print_update(attempts, page_number, len(df), sum(total_records), response.status_code, message)
                    sleep(self.wait)
                    attempts += 1
                    if attempts >= max_attempts: break

                elif not response.ok:

                    # Page possibly not found wait and retry 
                    message = f'Server not responding with appropriate data. Waiting {self.wait} seconds before trying again'
                    print_update(attempts, page_number, len(df), sum(total_records), response.status_code, message)
                    attempts += 1
                    if attempts >= max_attempts: break
                    sleep(self.wait)
            
            if len(dfs) > 0:
                df_concat = pd.concat(dfs, ignore_index=True)
                df_concat.index.name = 'index'
                df_concat.to_csv(f'{os.getcwd()}/data/{town}/{self.formatSearchWord()}.csv')
            else: df_concat = pd.DataFrame()

    # Search all directories on the data folder looking for appropriate results 
    def combineCSVs(self):

        # Create an empty list for holding DataFrames
        dfs = []

        # Iterate through the town folders gathering csvs matching searches
        directories = glob.glob(f'{os.getcwd()}/data/*/')
        for directory in directories:
            csvs = glob.glob(f'{directory}{self.formatSearchWord()}_kw.csv')
            for csv in csvs:
                df = pd.read_csv(csv, index_col='index', dtype={'telephone': str})
                dfs.append(df)

        df_concat = pd.concat(dfs, ignore_index=True)
        df_concat = df_concat.drop_duplicates(subset=['listing_url', 'telephone'])
        df_concat['telephone'] = df_concat['telephone'].astype(str)
        for index, row in df_concat.iterrows():
            df_concat.loc[index, 'telephone'] = f"'{row.telephone}"
        df_concat.index.name = 'index'
        df_concat.to_csv('master_csv.csv')

# Define settings for the class on initialisation
settings = {

    # Enable only one 'top_towns' or 'specific_towns', True = on False = off
    'top_towns': True,
    'specific_towns': False,

    'n_towns': 5, # Number of top towns to search 
    'ts_search_word': 'Pet Shop', # Search term for yell.com
    'ts_radius': '10', # Miles from centre of town to search 
    'town_names': [ # Used when 'specific towns' is on, list of towns to search
        'Edinburgh'
    ],
    'keywords': [ # Keywords to search the company listing for
        'dog',
        'hamster',
        'reptile'
    ],
    'attempts': 3, # Maximum number of attempts at returning data before moving on
    'wait': 120 # How long in seconds to wait between attempts
}

# Process split into 4 section
scraper = Scraper(settings)
# scraper.searchTowns()
scraper.addKeywords()
scraper.combineCSVs()

