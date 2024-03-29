import os
import yaml
import subprocess
from bs4 import BeautifulSoup # For parsing HTML
import random

def parse_page_content(page_data, page_url):
    """
    Extracts CSV links from HTML page data for a specific year.

    Args:
        page_data (str): HTML page data containing links.
        page_url (str): The Base Url given for the page

    Returns:
        list: A list of CSV file URLs.
    """
    # Initialize an empty list to store the extracted CSV file URLs
    res = []

    # Parse the HTML page using BeautifulSoup
    soup = BeautifulSoup(page_data, 'html.parser')

    # Find all hyperlinks in the HTML page
    hyperlinks = soup.find_all('a')

    # Iterate through each hyperlink
    for link in hyperlinks:
        # Get the 'href' attribute of the hyperlink
        href = link.get('href')

        # Check if the href contains ".csv"
        if ".csv" in href:
            # Create the full CSV file URL by combining the base page URL and href
            file_url = f'{page_url}{href}'
            
            # Append the CSV file URL to the result list
            res.append((file_url,href.rstrip(".csv")))

    # Return the list of extracted CSV file URLs
    return res

def download_csv(n_locs, year, seed):
    # Load parameters from params.yaml
    with open('params.yaml', 'r') as params_file:
        params = yaml.safe_load(params_file) #["download"] if file specific params

    # Extract relevant parameters
    n_locs = params.get('n_locs', n_locs)
    year = params.get('year', year)
    random.seed(params.get('seed', seed))

    # Extract Available csv files URL from the base html url
    html_url = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/"
    html_pages_content = subprocess.check_output(['curl', html_url])
    all_locations = parse_page_content(html_pages_content,page_url=html_url)
    location_urls = random.sample(all_locations, int(n_locs))

    # Create a folder to store downloaded files
    output_folder = f'data/csv/{year}'
    os.makedirs(output_folder, exist_ok=True)

    # Download CSV files for each location
    for url,location in location_urls:
        output_file = os.path.join(output_folder, f"data_{location}_{year}.csv")

        # Use curl to download the file
        if not os.path.exists(output_file):
            subprocess.run(['curl', url, '-o', output_file])

if __name__ == "__main__":
    # Set default values for n_locs and year
    n_locs = 10
    year = 2007
    seed = 40

    # Call the download_csv function
    download_csv(n_locs, year, seed)