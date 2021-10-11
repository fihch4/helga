from bs4 import BeautifulSoup
import asyncio
import aiohttp
from config import *
import re
from mysql_script import MySQLi
import datetime


async def get_page_data(session, page, urls_database_dict):
    """
    This is main task
    """
    regex = re.compile('.*cart track-block.*')
    response = await session.get(url=page, headers=headers)
    # print(page)
    if response.status == 200 and '/katalog/' in page:
        soup = BeautifulSoup(await response.text(), 'lxml')
        products_count_real = len(soup.findAll("div", {"class": regex}))  # Количество товаров на странице пагинации
        products_count_html_counter = soup.find("span", {
            "class": 'how_much_founded'})  # Количество товаров, которое указано рядом со страницей сортировки
        products_count_html_counter = products_count_html_counter.text.replace(',', '')
        print(f"Количество товаров на странице пагинации: {products_count_real}\n"
              f"Количество товаров в категории: {products_count_html_counter}\n"
              f"ID DATABASE: {urls_database_dict[page]}\n"
              f"URL: {page}")
        date_now = datetime.datetime.now().date()
        db.commit("INSERT INTO count_products (date, id_url, count_prod, count_html_prod, url) VALUES (%s, %s, %s, %s, %s)"
                  , date_now, urls_database_dict[page], products_count_real, products_count_html_counter, page)


async def gather_data(urls_dabase_dict):
    """
    This is tasks list
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url_for_parse in urls_dabase_dict:
            task = asyncio.create_task(get_page_data(session, url_for_parse, urls_dabase_dict))
            tasks.append(task)

        await asyncio.gather(*tasks)


# def main():
#     asyncio.run(gather_data())


if __name__ == '__main__':
    db = MySQLi(db_host, db_user, db_password, db_name)
    distinct_urls_for_parse = db.fetch("SELECT DISTINCT url, id_url FROM metrika_urls")
    distinct_urls_for_parse = distinct_urls_for_parse['rows']
    urls_dict = {}
    for url in distinct_urls_for_parse:
        # print(url)
        url_data_base = url[0]
        id_url_dabase = url[1]
        urls_dict[url_data_base] = id_url_dabase
    asyncio.run(gather_data(urls_dict))

