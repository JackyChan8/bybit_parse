import csv
import sys
import time
import os.path
import requests
import schedule
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, wait

base_url = 'https://announcements.bybit.com'
filename = 'data/data.csv'


# Files
def check_exist_file() -> bool:
    """Check Exist Data CVS File"""
    return os.path.isfile(filename)


def write_to_file(data):
    """Write to CSV File"""
    try:
        with open(filename, 'a', encoding='utf-8', newline='') as f:
            fieldnames = ['link', 'title', 'publisher']
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if f.tell() == 0:
                writer.writeheader()

            for row in data:
                writer.writerow({
                    'link': row['link'],
                    'title': row['title'],
                    'publisher': row['publisher']
                })
    except Exception as e:
        print('Exception write to file: ', e)


def check_article(articles):
    """Read From CSV File"""
    new_data = []
    try:
        df = pd.read_csv(filename)
        for article in articles:
            out = df['title'].eq(article['title']).any()
            if not out:
                new_data.append(article)

        write_to_file(new_data)
    except Exception as e:
        print('Check article from csv: ', e)


# Driver Browser
def options_browser():
    """Options Selenium Browser"""
    options = Options()
    options.add_argument("-headless")
    return options


def get_driver():
    driver = webdriver.Firefox(options=options_browser())
    return driver


def get_user_agent() -> str:
    """Get User Agent"""
    ua = UserAgent()
    return ua.random


# Articles Data
def check_exist_new_article():
    """Check Exists New Articles"""
    try:
        browser = get_driver()
        if connect_to_base(browser, 1):
            time.sleep(2)
            html = browser.page_source
            articles = parse_html(html)
            browser.quit()
            # Check Exists And Add New Articles
            check_article(articles)
        else:
            print('Error connecting to website')
            browser.quit()
    except Exception as e:
        print('Exception check exist new article: ', e)


def connect_to_base(browser, page_number):
    """Connection WebSite"""
    connection_attempts = 0
    while connection_attempts < 3:
        try:
            browser.get(base_url + f'/en-US/?category=&page={page_number}')
            # ожидаем пока элемент будет загружен на странице
            # затем функция вернет True иначе False
            WebDriverWait(browser, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "article-list"))
            )
            return True
        except Exception as e:
            print(e)
            connection_attempts += 1
            print(f"Error connecting to {base_url}.")
            print(f"Attempt #{connection_attempts}.")
    return False


# Parsing
def get_count_pages() -> int:
    """Get Count Pages"""
    try:
        headers = {
            'User-Agent': get_user_agent()
        }
        res = requests.get(base_url, headers=headers)
        soup = BeautifulSoup(res.content, 'html.parser')
        pagination = soup.find('ul', class_='ant-pagination')
        pagination_items = pagination.find_all('li', class_='ant-pagination-item')
        return int(pagination_items[-1]['title'])
    except Exception as e:
        print('Exception get count pages: ', e)
        sys.exit(1)


def parse_html(html) -> list:
    """Parse HTML Page"""
    try:
        soup = BeautifulSoup(html, "html.parser")
        articles = []
        articles_block = (soup.find('div', class_='article-list')
                          .find_all('a', href=True))
        for article in articles_block:
            link = base_url + article['href']
            title = article.find('div', class_='article-item-title').find('span').text
            publisher = article.find('div', class_='article-item-date').text
            articles.append({
                'link': link,
                'title': title,
                'publisher': publisher
            })
        return articles
    except Exception as e:
        print('Exception parse html: ', e)


# Run Function
def run(page_number):
    browser = get_driver()
    if connect_to_base(browser, page_number):
        time.sleep(2)
        html = browser.page_source
        articles = parse_html(html)
        write_to_file(articles)
        browser.quit()
    else:
        print('Error connecting to website')
        browser.quit()


def main():
    try:
        exist_file_csv = check_exist_file()
        print(exist_file_csv)
        if not exist_file_csv:
            count = get_count_pages()
            futures = []

            with ThreadPoolExecutor(4) as executor:
                for page in range(count, 0, -1):
                    futures.append(executor.submit(run, page))
            wait(futures)
        else:
            check_exist_new_article()
    except KeyboardInterrupt:
        return schedule.CancelJob


if __name__ == '__main__':
    try:
        schedule.every(1).seconds.do(main)
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit(1)
