from tapi_yandex_metrika import YandexMetrikaStats
from config import *
import json
from mysql_script import MySQLi
import datetime

# По умолчанию возвращаются только 10000 строк отчета,
# если не указать другое кол-во в параметре limit.
# В отчете может быть больше строк, чем указано в limit
# Тогда необходимо сделать несколько запросов для получения всего отчета.
# Чтоб сделать это автоматически вы можете указать
# параметр receive_all_data=True при инициализации класса.
api = YandexMetrikaStats(
    access_token=ACCESS_TOKEN,
    # Если True, будет скачивать все части отчета. По умолчанию False.
    receive_all_data=True
)

params = dict(
    ids=METRIC_IDS,
    # metrics = "ym:s:users",
    metrics="ym:s:users,ym:s:visits",
    # dimensions = "ym:s:date,ym:s:<attribution>TrafficSource",
    # dimensions = "ym:s:date,ym:s:<attribution>TrafficSource,ym:s:startURL",
    dimensions="ym:s:startURL,ym:s:<attribution>TrafficSource",
    date1="2daysAgo",
    date2="yesterday",
    # sort="ym:s:date",
    accuracy="full",
    limit=200
)


def main():
    date_now = datetime.datetime.now().date()
    db = MySQLi(db_host, db_user, db_password, db_name)
    result = api.stats().get(params=params)
    result = result().data
    result = result[0]['data']

    for i in result:
        # print(i)
        url = i['dimensions'][0]['name']
        type_source = i['dimensions'][1]['name']
        users = i['metrics'][0]
        visits = i['metrics'][1]
        if type_source == 'Search engine traffic' and float(visits) >= 4:
            url_from_db = db.fetch("SELECT url FROM metrika_urls WHERE url = %s AND date = %s", url, date_now)
            if int(len(url_from_db['rows'])) <= 0:
                print("Добавлен новый URL")
                db.commit("INSERT INTO metrika_urls (url, visits, users, date) VALUES (%s, %s, %s, %s)", url, visits, users, date_now)


if __name__ == '__main__':

    main()
