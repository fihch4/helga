from mysql_script import MySQLi
from config import *
import datetime
import urllib3
import requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
telegram_url = 'https://api.telegram.org/bot' + bot_telegram_token + '/' + 'sendmessage?chat_id=' + telegram_chat_id + '&text= '


def main():
    db = MySQLi(db_host, db_user, db_password, db_name)
    old_dictionary_urls = {}
    urls_from_data_base = db.fetch("SELECT DISTINCT url FROM count_products")
    urls_str_for_telegram = '⚠Achtung\n'
    for url in urls_from_data_base['rows']:
        url_for_date = url[0]
        old_date_count = db.fetch(
            "SELECT date, count_html_prod, id_url FROM count_products WHERE url = %s ORDER BY date ASC LIMIT 1",
            url_for_date)
        old_date = old_date_count['rows'][0][0]
        count_products_old = old_date_count['rows'][0][1]
        id_url = old_date_count['rows'][0][2]

        actual_date_count = db.fetch(
            "SELECT date, count_html_prod FROM count_products WHERE url = %s ORDER BY date DESC LIMIT 1", url_for_date)
        actual_date = actual_date_count['rows'][0][0]
        count_products_actual = actual_date_count['rows'][0][1]

        percent_actual = int(count_products_actual) * 100 / int(count_products_old)
        diff_percent = 100 - percent_actual
        date_now = datetime.datetime.now().date()
        db.commit("INSERT INTO percent_diff (date, url, diff_percent, id_url) VALUES (%s, %s, %s, %s)", date_now,
                  url_for_date, diff_percent, id_url)
        urls_list_len = []
        if diff_percent >= 50:
            print(f"OLD_DATE: {old_date}\n"
                  f"ACTUAL_DATE: {actual_date}\n"
                  f"PERCENT_ACTUAL: {percent_actual}\n"
                  f"DIFF: {diff_percent}")
            urls_str_for_telegram += f"URL: {url_for_date} Было: {count_products_old} pcs. | Стало: {count_products_actual} pcs.\n"
            urls_list_len.append(url_for_date)

    if len(urls_list_len) >= 1:
        request_url = telegram_url + urls_str_for_telegram
        requests.get(request_url, verify=False)

    # old_dictionary_urls[url_for_date] = {old_date : count_products_old}
