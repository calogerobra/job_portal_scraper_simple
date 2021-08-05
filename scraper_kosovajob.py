# Import general libraries
import datetime
import pandas as pd
from bs4 import BeautifulSoup as soup
import time

import requests
import os
requests.packages.urllib3.disable_warnings()
import random

def adjust_listings_pages(page, pagelist):
    """ Adjusts listings to restart properly after crash
    Args:
        Current page
        Amount of pages
        Parameter if query is set on repeat
    Returns:
        Jumps to page where last stopped
    """
    return pagelist[pagelist.index(page):len(pagelist)]


def join_url(*args):
    """Constructs an URL from multiple URL fragments.
    Args:
        *args: List of URL fragments to join.
    Returns:
        str: Joined URL.
    """
    return '&'.join(args)

def construct_listing_url(base_url):
    """Constructs URLs for listing parameters.
    Args:
        key_word: Key word for job search
        job_category: category of job (numeric input from webpage)
        region: region for job (numeric entry from webpage, all is -1)
    Returns:
        str: An URL for the requested listing parameters.
    """
    # modify this here with exceptions
    constructed_url = base_url
    return constructed_url

def request_page(url_string, verification):
    """HTTP GET Request to URL.
    Args:
        url_string (str): The URL to request.
    """
    #time.sleep(random.randint(1,3))
    uclient = requests.get(url_string, timeout = 60, verify = verification)
    page_html = uclient.text
    return page_html

def set_max_page(page_soup):
    """Sets the maximum page number of the current search.
    Args:
        page_soup element
    Returns:
        Amount of pages of the current query
    """
    return 1

def create_elements(container, idcount):
    """Extracts the relevant information form the html container, i.e. object_id,
    Args:
        A container element + region, city, districts, url_string.
    Returns:
        A dictionary containing the information for one listing.
    """
    object_link = str(container.a['href'])
    object_title = container.findAll("div", {"class": "listsPosition"})[0].text
    city = container.findAll("div", {"class": "listsCity"})[0].text
    object_id = str(idcount)
    # Create a dictionary as output
    return dict([("object_link", object_link),
                 ("object_title", object_title),
                 ("city", city),
                 ("object_id", object_id)])

def reveal_link(input_dict):
    """ Reveals the object link of the listing currently in loop
    Args:
        input dictionary from elements creator
    Returns:
        object id (also referred to as listing id or object id)
    """
    return input_dict['object_link']

def reveal_id(input_dict):
    """ Reveals the object id of the listing currently in loop
    Args:
        input dictionary from elements creator
    Returns:
        object id (also referred to as listing id or object id)
    """
    return input_dict['object_id']


def make_listings_soup(object_link, verification):
    """ Create soup of listing-specific webpage
    Args:
        object_link
    Returns:
        soup element containing listings-specific information
    """
    listing_url = object_link
    return soup(request_page(listing_url, verification), 'html.parser')


def add_contents(listing_soup, dpath, now, idcount):
    """ Add contents from the listing to the current input dictionary
    Args:
        A listing bs4 elemnt, listing_soup
        The corresponding listing id, object_id
        A path for where possible content files shall be stored
    Returns:
        Dictionary with listings-specific content
    """
    # Set extraction time
    # 1. Extract the main header information with company content
    top_right_container= listing_soup.findAll("div", {"class", "containerRightAreaTopArea"})[0]
    views = top_right_container.findAll("div", {"class": "listingArea listingArea3 listingAreaTopComp"})[0].b.text
    job_category = top_right_container.findAll("div", {"class": "listingArea listingArea1 listingArea3Cat"})[0].b.text
    try:
        days_due = int(top_right_container.findAll("div", {"class": "listingArea listingArea2 listingArea3Exp"})[0].b.text.replace(" ditë ",""))
        expiration_date = (datetime.date.today() + datetime.timedelta(days=days_due)).strftime('%Y-%m-%d')
    except ValueError:
        expiration_date = datetime.date.today().strftime('%Y-%m-%d')
    type_of_contract = top_right_container.findAll("div", {"class": "listingArea listingArea3 listingArea3Orar"})[0].b.text
    job_description = listing_soup.findAll("div", {"class": "containerLeftAreDescription"})[0].text
    try:
        company_name = listing_soup.findAll("b", {"class": "containerLeftAreaTopAreaRightTitleComp"})[0].text
    except:
        company_name = listing_soup.findAll("div", {"class": "containerLeftAreaTopAreaRightTitleComp containerLeftAreaTopAreaRightTitleCompT"})[0].text
    try:
        link = listing_soup.findAll("div", {"class": "containerLeftAreDescription"})[0].p.img['src']
        # Check data type first to export correct format
        check_dtype = link.split(".")
        dtype = "." + check_dtype[len(check_dtype)-1]
        has_document = 1
        response = requests.get(link, timeout = 60, verify = False)
        if response.status_code == 200:
            filename = dpath + str(now) + "_" + str(idcount) + dtype
            with open(filename, 'wb') as f:
                f.write(response.content)
    except (TypeError, AttributeError, requests.exceptions.ConnectionError):
        has_document = 0
        #print("Link could not be read out, skipping...")
        pass
    except KeyError:
        print("Key error occurred while extracting document, skipping ...")
        has_document = 0
        pass
    except requests.exceptions.MissingSchema:
        print("Could not read image link in listing, skipping ...")
        has_document = 0
        pass
    # 3. Export dictionary
    return  dict([('company_name', company_name),
                  ('expiration_date', expiration_date),
                  ('job_category', job_category),
                  ('job_description', job_description),
                  ('has_document', has_document),
                  ('type_of_contract', type_of_contract),
                  ('views', views)])

def save_html_to_text(listing_soup, listing_textfile_path, now_str, now, idcount):
    """ Saves each listing as backup in a seperate text file.
    Args:
        beautioful soup elemnt for listing
        output path
        now string for timestamp
        object_id for link to SQL table
    """
    listing_soup_b = listing_soup.prettify("utf-8")
    time_folder = listing_textfile_path + now_str
    tfile_name = time_folder + "\\" + now + "_" + str(idcount) + "_listing" + ".txt"
    with open(tfile_name, "wb") as text_file:
        return text_file.write(listing_soup_b)

def scrape_kosovajob(maxpage, max_repeats, dpath, base_url, verification, now_str, listing_textfile_path):
    """Scraper for Kosovajob job portal based on specified parameters.
    In the following we would like to extract all the containers containing
    the information on one listing. For this purpose we try to parse through
    the html text and search for all elements of interest.
    Args:
        input parameters dor search (key_word, job_category, region)
        maxpage for initial start
        path for pdf and jpeg files
        base_url for start
        verification input for ssl certificate
        current time stamp (now_str)
    Returns:
        Appended pandas dataframe with crawled content.
    """
    # Define dictionary for output
    input_dict = {}
    frames = []
    on_repeat = False
    first_run = True
    counter = 0
    skipper = 0
    idcount = 0
    pagelist = list(range(1,maxpage))
    # Loop over pages
    while on_repeat or first_run:
        counter += 1
        if counter >= max_repeats:
            break
        print("Running iteration", counter, "of parser ...")
        try:
            for page in pagelist:
                # 1. Correct pagecount
                pagelist = adjust_listings_pages(page, pagelist)
                # 2. Set URL string
                url_string = construct_listing_url(base_url)
                # 3. Now let's parse through the page that we previously stored and do the scraping
                time.sleep(random.randint(0,1))
                page_soup = soup(request_page(url_string, verification), 'html.parser') # datatype from beautiful soup
                # 4. Grab pagecount to avoid repititions
                maxpage = set_max_page(page_soup)
                # 5. Grab all listings on the page
                print("Reading page", page, "of", maxpage, "...")
                containers = page_soup.findAll("div" , {"class":"lists"})
                len(containers) # verifies that we have e.g. 20 postings
                # 6. Iterate over containers
                print("Reading out", len(containers), "containers..." )
                for container in containers:
                    now = datetime.datetime.now()
                    now_str2 = now.strftime("%Y%m%d_%H%M%S")
                    try:
                        idcount += 1
                        # 6.1. Create a dictionary with main content of front page
                        input_dict = create_elements(container, idcount)
                        # 6.2 Create soup for individual listing
                        time.sleep(random.randint(0,1))
                        listing_soup = make_listings_soup(reveal_link(input_dict), verification)
                        # 6.4. Add further contents
                        input_dict.update(add_contents(listing_soup, dpath, now_str2, idcount))
                        # 6.5 Save listing
                        save_html_to_text(listing_soup, listing_textfile_path, now_str, now_str2, idcount)
                        # 6.6. Create a dataframe
                        df = pd.DataFrame(data = input_dict, index =[now])
                        df.index.names = ['scraping_time']
                        frames.append(df)
                        time.sleep(1)
                    except (IndexError, ValueError, TypeError):
                        print("Encountered problem, skipping container...")
                        skipper += 1
                        print("No. of skips so far: ", skipper)
                        time.sleep(1)
                        continue
                # 7. Break out of loop if maxpage is hit
                if page == maxpage:
                    break # Break loop to avoid repititions once maxpage is hit
            first_run = False
            on_repeat = False
            return pd.concat(frames)
        except requests.exceptions.ConnectionError:
            print("Connection was interrupted, waiting a few moments before continuing...")
            time.sleep(random.randint(2,5) + counter)
            on_repeat = True
            continue

def main():
    """ Note: Set parameters in this function
    """
    # Set now string
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Set base URL
    base_url = 'https://www.kosovajob.com'

    # Set verification setting for certifiates of webpage. Check later also certification
    verification = True

    # Set dpath for image and pdf outputs
    dpath = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_scraper_simple\\data\\listing_files\\"
    listing_textfile_path = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_scraper_simple\\data\\daily_scraping\\single_listings_htmls\\"

    # Create folder for listing output files
    time_folder = listing_textfile_path + now_str
    os.mkdir(time_folder)

    # Set max amount of pages to be crawled, 20 is default here
    maxpage = 1000000

    # Set maximum amount of repeats before ending
    max_repeats = 20

    # Execute functions for scraping
    start_time = time.time() # Capture start and end time for performance
    appended_data = scrape_kosovajob(maxpage, max_repeats, dpath, base_url, verification, now_str, listing_textfile_path)
    # Write output to Excel
    print("Writing to Excel file...")
    time.sleep(1)
    file_name = '_'.join(['C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_scraper_simple\\data\\daily_scraping\\' +
    str(now_str), 'kosovajob.xlsx'])
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    appended_data.to_excel(writer, sheet_name = 'jobs')
    writer.save()
    workbook = writer.book
    worksheet = writer.sheets['jobs']
    format1 = workbook.add_format({'bold': False, "border" : True})
    worksheet.set_column('A:M', 15  , format1)
    writer.save()

    # Check end time
    end_time = time.time()
    duration = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))

    # For interaction and error handling
    final_text = "Your query was successful! Time elapsed:" + str(duration)
    print(final_text)
    time.sleep(2)

# Execute scraping
if __name__ == "__main__":
    main()



