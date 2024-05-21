from time import sleep
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from sklearn.preprocessing import LabelEncoder
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def fetch_page_source(url):
    try:
        with webdriver.Chrome() as driver:
            driver.get(url)
            return driver.page_source
    except Exception as e:
        logging.error(f"Error fetching page source for URL: {url}")
        logging.exception(e)
        return None
    
def extract_links(page_source):
    try:
        soup = BeautifulSoup(page_source, "html.parser")
        links = soup.findAll('a', 'js__product-link-for-product-id')
        return [{'link': f"https://batdongsan.com.vn{link.get('href')}"} for link in links]
    except Exception as e:
        logging.error("Error extracting links from page source")
        logging.exception(e)
        return []
    
def crawl_page(url):
    logging.info(f"Crawling URL: {url}")
    page_source = fetch_page_source(url)
    if not page_source:
        logging.error(f"No page source found for URL: {url}")
        return []

    links = extract_links(page_source)
    if not links:
        logging.error(f"No links found for URL: {url}")
    return links

def crawl_links(base_url, page_range):
    all_links = []
    urls = [f"{base_url}/p{i}" for i in page_range]

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(crawl_page, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                links = future.result()
                all_links.extend(links)
                logging.info(f"Done processing URL: {url}")
            except Exception as e:
                logging.error(f"Error processing URL: {url}")
                logging.exception(e)

    return all_links

def save_links_to_csv(links, filename):
    df = pd.DataFrame(links)
    df.to_csv(filename, index=False)

def crawlLinks():
    dt = crawl_links('https://batdongsan.com.vn/ban-nha-dat-ha-noi', range(1, 1000))
    save_links_to_csv(dt, 'data/links/link_data.csv')

def extract_type_estate_and_prId(url):
    # Define regular expressions for matching prId
    prId_pattern = r"pr(\d+)"

    # Match prId using regular expression
    prId_match = re.search(prId_pattern, url)

    # Extract prId if a match is found
    prId = prId_match.group(1) if prId_match else None

    # Extract type_estate based on specific strings in the URL
    type_estate_keywords = {
        'ban-nha-rieng': 'Nhà riêng',
        'ban-nha-mat-pho': 'Nhà mặt phố',
        'ban-nha-biet-thu-lien-ke': 'Nhà biệt thự liền kề',
        'ban-shophouse-nha-pho-thuong-mai': 'Shophouse/Nhà phố thương mại'
    }

    type_estate = 'Unknown'
    for keyword, type_name in type_estate_keywords.items():
        if keyword in url:
            type_estate = type_name
            break

    return type_estate, prId

def extract_property_details(url):
    property_details = {
        'pr_id': '',
        'type_estate': '',
        'district': '',
        'posted_date': '',
        'area': '',
        'price': '',
        'legal_document': '',
        'interior': '',
        'num_bedrooms': '',
        'num_bathrooms': '',
        'num_floors': '',
        'house_orientation': '',
        'balcony_orientation': '',
        'entrance': '',
        'frontage': ''
    }

    property_details['type_estate'], property_details['pr_id'] = extract_type_estate_and_prId(url)

    try:
        # Set up Selenium WebDriver
        with webdriver.Chrome() as driver:
            driver.get(url)
            
            # Get the page source
            page_source = driver.page_source

        soup = BeautifulSoup(page_source, "html.parser")

        try:
            # Find the breadcrumb element
            breadcrumb = soup.find('div', class_='re__breadcrumb')
            if breadcrumb:
                level_3_link = breadcrumb.find('a', {'level': '3'})
                if level_3_link:
                    property_details['district'] = level_3_link.text
            else:
                print(f"Breadcrumb not found for URL: {url}")
        except AttributeError as e:
            print(f"Error extracting district: {e}")

        # Extract posted date
        try:
            posted_date_span = soup.find('span', string='Ngày đăng')
            if posted_date_span:
                property_details['posted_date'] = posted_date_span.find_next_sibling('span').text.strip()
        except AttributeError as e:
            print(f"Error extracting posted_date: {e}")

        # Extract property specifications
        try:
            specs_content = soup.find_all('div', class_='re__pr-specs-content-item')
            # Mapping of titles to corresponding keys in property_details
            title_map = {
                'Diện tích': 'area',
                'Mức giá': 'price',
                'Pháp lý': 'legal_document',
                'Nội thất': 'interior',
                'Số phòng ngủ': 'num_bedrooms',
                'Số toilet': 'num_bathrooms',
                'Số tầng': 'num_floors',
                'Hướng nhà': 'house_orientation',
                'Hướng ban công': 'balcony_orientation',
                'Đường vào': 'entrance',
                'Mặt tiền': 'frontage'
            }

            # Extract and map property details
            for item in specs_content:
                title = item.find(class_='re__pr-specs-content-item-title').text.strip()
                if title in title_map:
                    property_details[title_map[title]] = item.find(class_='re__pr-specs-content-item-value').text.strip()
        except AttributeError as e:
            print(f"Error extracting property details: {e}")
        
    except Exception as e:
        print(f"Failed to extract data from {url}: {e}")

    return property_details

# Function to write property details to CSV
def write_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, mode='a', header=not pd.io.common.file_exists(filename))

# Function to get existing pr_id from CSV
def get_existing_pr_ids(filename):
    if pd.io.common.file_exists(filename):
        df = pd.read_csv(filename)
        print(df)
        existing_pr_ids = df['pr_id'].astype(str).tolist()
        return existing_pr_ids
    return []

# Function to filter URLs based on existing pr_id
def filter_urls(urls, existing_pr_ids):
    filtered_urls = []
    for url in urls:
        try:
            pr_id = extract_type_estate_and_prId(url)[1]
            if pr_id not in existing_pr_ids:
                filtered_urls.append(url)
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
    return filtered_urls

def scrape_property_details(urls, filename):
    existing_pr_ids = get_existing_pr_ids(filename)
    print('existing_pr_ids', existing_pr_ids)
    # Filter URLs that are not in existing_pr_ids
    filtered_urls = filter_urls(urls, existing_pr_ids)
    print('filtered_urls', filtered_urls)
    
    scraped_properties = []
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(extract_property_details, url): url for url in filtered_urls}
        try:
            for future in as_completed(futures):
                url = futures[future]
                try:
                    result = future.result()
                    scraped_properties.append(result)
                    write_to_csv([result], filename)
                except Exception as e:
                    print(f"Failed to extract data from {url}: {e}")
        except KeyboardInterrupt:
            for f in futures:
                f.cancel()
            raise

    return scraped_properties


def crawlData():
    crawlLinks()
    df_link_data = pd.read_csv('data/raws/link_data.csv')
    LIST_LINK_PRODUCT = df_link_data['link'].values.tolist()
    df = pd.read_csv('data/raws/raw_data.csv')
    critical_fields = ['district', 'posted_date', 'area', 'price']
    df.dropna(subset=critical_fields, how='all', inplace=True)
    df.to_csv('data/raws/raw_data.csv',index=False)
    LIST_PRODUCT = scrape_property_details(LIST_LINK_PRODUCT, 'data/raws/raw_data.csv')
    print(LIST_PRODUCT)
