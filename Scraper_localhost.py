# -*- coding: UTF-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import time
import threading

# URL mapping structure for full scale
url_mapping = {
    '光伏': {
        'base': 'https://guangfu.bjx.com.cn/',
        'keywords': {
            'zc/': '光伏政策',
            'sc/': '光伏市场',
            'mq/': '光伏企业',
            'dj/': '光伏观点',
            'xm/': '光伏项目',
            'zb/': '光伏招标采购',
            'cj/': '光伏财经',
            'gj/': '光伏国际',
            'sj/': '光伏价格',
            'js/': '光伏技术'
        }
    },
    '风电': {
        'base': 'https://fd.bjx.com.cn/',
        'keywords': {
            'zc/': '风电政策',
            'sj/': '风电数据',
            'sc/': '风电市场',
            'mq/': '风电企业',
            'zb/': '风电招标',
            'js/': '风电技术',
            'bd/': '风电报道',
            'pl/': '风电评论',
            'rw/': '风电人物',
            'fdcy/': '风电产业',
            'xm/': '风电项目',
            'fdsbycl/': '风电设备',
            'fdyw/': '风电运维',
            'ypjycl/': '叶片及原材料',
            'hsfd/': '海上风电',
            'gj/': '全球风电'
        }
    }
}

# MySQL database connection information
db_host = 'localhost'
db_user = 'root'
db_passwd = 'e8NcFY6d'
db_port = 3306
db_name = 'math'

# SQLAlchemy engine
engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(db_user, db_passwd, db_host, db_port, db_name), echo=True)


# Function to simulate network delay
def delay(t):
    time.sleep(t)

# Function to print output to the console
def out_print(s):
    print(s)

# Function to extract article content and keywords from a news URL
def extract_meta_data_and_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        out_print(f"请求错误：{e}")
        return None, None

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        keywords = soup.find('meta', attrs={'name': 'Keywords'})
        keywords_content = keywords['content'] if keywords else 'N/A'

        # Extracting the article content based on the provided HTML structure
        article_div = soup.find('div', class_='cc-article')
        paragraphs = article_div.find_all('p') if article_div else []
        article_content = '\n'.join([p.get_text() for p in paragraphs if not p.find('img')])

        return keywords_content, article_content
    else:
        return None, None

# Crawler main function
def crawler(url_base, keyword, category, start_page=1, end_page=100):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    data_list = []

    # Construct URL and start crawling
    out_print(f"正在解析 {category} 的页面数据...")
    for page_num in range(start_page, end_page + 1):
        page_start_time = time.time()  # 开始计时

        url = f"{url_base}{keyword}{page_num}"
        try:
            response = requests.get(url, headers=headers, timeout=5)  # 缩短超时时间
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            out_print(f"请求错误：{e}")
            continue

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.select('div.cc-list-content ul li')

            for article in articles:
                title = article.a.get('title')
                href = article.a.get('href')
                date = article.span.text
                keywords, content = extract_meta_data_and_content(href)
                data_list.append({
                    '政策': f"{date} {title}",
                    '网址链接': href,
                    'Keywords': keywords,
                    '内容': content
                })
        else:
            break

        page_end_time = time.time()  # 结束计时
        out_print(f"页面 {page_num} 解析时间: {page_end_time - page_start_time:.2f} 秒")

    # Save data to MySQL
    if data_list:
        out_print("爬取完成，开始写入数据到MySQL...")
        df = pd.DataFrame(data_list)
        table_name = f"`{category}`"

        try:
            with engine.connect() as connection:
                connection.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                                        "`政策` TEXT, "
                                        "`网址链接` TEXT, "
                                        "`Keywords` TEXT, "
                                        "`内容` TEXT)"))

                # Check for duplicates and remove them
                existing_data = pd.read_sql(f"SELECT `政策` FROM {table_name}", connection)
                new_data = df[~df['政策'].isin(existing_data['政策'])]

                if not new_data.empty:
                    new_data.to_sql(table_name.strip('`'), con=connection, if_exists='append', index=False)
                    out_print(f"数据写入完成：{category}")
                    out_print(f"写入了 {len(new_data)} 条数据到 {category} 表")

                    # Output the start and end dates
                    start_date = new_data['政策'].apply(lambda x: x.split(' ')[0]).min()
                    end_date = new_data['政策'].apply(lambda x: x.split(' ')[0]).max()
                    out_print(f"数据日期范围：{start_date} 至 {end_date}")
                else:
                    out_print("没有新数据需要写入。")
        except Exception as e:
            out_print(f"数据写入失败：{e}")
    else:
        out_print("没有找到数据。")

# Function to start the crawler
def start_crawler():
    start_time = time.time()  # 开始计时

    start_page, end_page = 1, 100  
    
    threads = []
    for main_cat in url_mapping.keys():
        url_base = url_mapping[main_cat]['base']
        for keyword, category in url_mapping[main_cat]['keywords'].items():
            thread = threading.Thread(target=crawler, args=(url_base, keyword, category, start_page, end_page))
            thread.start()
            threads.append(thread)
            delay(0.5)  # 降低延迟以更快地启动线程

    for thread in threads:
        thread.join()
    
    end_time = time.time()  # 结束计时
    out_print(f"爬虫已启动，总耗时: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    start_crawler()
