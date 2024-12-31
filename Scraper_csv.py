# -*- coding: UTF-8 -*-

import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from datetime import datetime
import random
from fake_useragent import UserAgent
import lxml.html
import logging
from typing import List, Dict, Any
import json

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URL mapping structure
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

class AsyncCrawler:
    def __init__(self):
        self.ua = UserAgent()
        self.session = None
        self.data_buffer = {}
        self.semaphore = asyncio.Semaphore(15)
        self.retry_count = 3
        self.success_count = 0  # 成功请求计数
        self.error_count = 0    # 错误请求计数

    def get_headers(self):
        """获取随机User-Agent和其他headers"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
        }

    def get_delay_time(self):
        """动态计算延迟时间"""
        if self.error_count > self.success_count * 0.2:  # 错误率超过20%
            return random.uniform(2, 4)
        return random.uniform(0.2, 0.5)

    async def fetch_page(self, url: str, session: aiohttp.ClientSession) -> str:
        """异步获取页面内容，添加重试机制"""
        for i in range(self.retry_count):
            try:
                async with self.semaphore:
                    delay = self.get_delay_time()
                    await asyncio.sleep(delay)
                    
                    async with session.get(url, headers=self.get_headers(), timeout=15) as response:
                        if response.status == 200:
                            self.success_count += 1
                            return await response.text()
                        elif response.status == 429:
                            self.error_count += 1
                            wait_time = 2 ** (i + 1)
                            logger.warning(f"触发限流，等待 {wait_time} 秒")
                            await asyncio.sleep(wait_time)
                            continue
            except Exception as e:
                self.error_count += 1
                logger.error(f"第{i+1}次获取页面失败 {url}: {str(e)}")
                if i < self.retry_count - 1:
                    await asyncio.sleep(2 ** i)
                    continue
        return None

    async def parse_article(self, html: str) -> tuple:
        """异步解析文章内容"""
        try:
            soup = BeautifulSoup(html, 'lxml')
            # 获取关键词
            keywords = soup.find('meta', attrs={'name': 'Keywords'})
            keywords_content = keywords['content'] if keywords else 'N/A'

            # 获取文章内容 - 使用与 Scraper_localhost.py 相同的解析逻辑
            article_div = soup.find('div', class_='cc-article')
            if not article_div:
                return None, None
            
            paragraphs = article_div.find_all('p')
            article_content = '\n'.join([p.get_text() for p in paragraphs if not p.find('img')])
            
            return keywords_content, article_content
        except Exception as e:
            logger.error(f"解析文章失败: {str(e)}")
            return None, None

    async def process_article(self, article_data: dict, session: aiohttp.ClientSession) -> dict:
        """处理单篇文章"""
        html = await self.fetch_page(article_data['href'], session)
        if html:
            keywords, content = await self.parse_article(html)
            if keywords and content:
                return {
                    '政策': f"{article_data['date']} {article_data['title']}",
                    '网址链接': article_data['href'],
                    'Keywords': keywords,
                    '内容': content
                }
        return None

    async def crawl_page(self, url: str, session: aiohttp.ClientSession) -> List[dict]:
        """爬取单个页面"""
        html = await self.fetch_page(url, session)
        if not html:
            return []

        try:
            soup = BeautifulSoup(html, 'lxml')
            articles_list = soup.select('div.cc-list-content ul li')
            
            articles = []
            for article in articles_list:
                try:
                    articles.append({
                        'title': article.a.get('title'),
                        'href': article.a.get('href'),
                        'date': article.span.text
                    })
                except Exception as e:
                    logger.error(f"解析文章列表项失败: {str(e)}")
                    continue
                
            return articles
        except Exception as e:
            logger.error(f"解析页面失败 {url}: {str(e)}")
            return []

    async def crawl_category(self, url_base: str, keyword: str, category: str, 
                            start_page: int, end_page: int) -> None:
        """优化的分类爬取方法"""
        async with aiohttp.ClientSession() as session:
            data_list = []
            page_num = start_page
            consecutive_empty = 0
            consecutive_errors = 0
            
            while page_num <= end_page:
                url = f"{url_base}{keyword}{page_num}"
                
                try:
                    articles = await self.crawl_page(url, session)
                    
                    if not articles:
                        consecutive_empty += 1
                        if consecutive_empty >= 5:
                            logger.info(f"{category} 连续5页无数据，在第 {page_num} 页停止")
                            break
                        page_num += 1
                        continue
                    
                    consecutive_empty = 0
                    consecutive_errors = 0
                    
                    batch_size = 10
                    for i in range(0, len(articles), batch_size):
                        batch = articles[i:i+batch_size]
                        tasks = [self.process_article(article, session) for article in batch]
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        valid_results = [r for r in results if isinstance(r, dict)]
                        data_list.extend(valid_results)
                        
                        # 动态调整批次间延迟
                        await asyncio.sleep(self.get_delay_time())
                    
                    logger.info(f"正在爬取 {category} - 第 {page_num} 页，当前数据量：{len(data_list)}")
                    page_num += 1
                    
                except Exception as e:
                    logger.error(f"爬取页面失败 {url}: {str(e)}")
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        await asyncio.sleep(5)
                        consecutive_errors = 0
                    continue

            if data_list:
                self.save_to_csv(data_list, category)

    def save_to_csv(self, data_list: List[Dict[str, Any]], category: str) -> None:
        """保存数据到CSV并输出日期范围"""
        df = pd.DataFrame(data_list)
        os.makedirs('data', exist_ok=True)
        filename = f'data/{category}.csv'
        
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            df = pd.concat([existing_df, df]).drop_duplicates(subset=['政策'])
        
        # 提取日期并找出范围
        dates = df['政策'].str.split(' ').str[0]
        earliest_date = min(dates)
        latest_date = max(dates)
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"{category} 爬取完成，共获取 {len(df)} 条数据")
        logger.info(f"数据日期范围：{earliest_date} 至 {latest_date}")

async def main():
    start_time = time.time()
    crawler = AsyncCrawler()
    
    max_concurrent_tasks = 6
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    
    async def bounded_crawl(url_base, keyword, category, start_page, end_page):
        async with semaphore:
            await crawler.crawl_category(url_base, keyword, category, start_page, end_page)
    
    tasks = []
    for main_cat, data in url_mapping.items():
        url_base = data['base']
        for keyword, category in data['keywords'].items():
            tasks.append(bounded_crawl(url_base, keyword, category, 1, 200))
    
    batch_size = 8
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        await asyncio.gather(*batch)
        await asyncio.sleep(1)
    
    total_time = time.time() - start_time
    logger.info(f"爬虫任务完成，总耗时: {total_time:.2f} 秒")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main()) 