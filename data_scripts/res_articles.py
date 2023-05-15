from _sql_scripts.sql_articles import *
from _sql_scripts.sql_pbn_servers import *


class PBNArticles():
    """
    1. Скрипт обращается к каждому пбн серверу из конфига
    2. Собирает список опубликованных статей и список запланированных к публикации статей
    3. Добавляет в БД отсутствующие статьи
    4. #TODO: Надо дописать обновление данных, например если какая-то статья была удалена или обновлена
    """
    def __init__(self):
        self.sql_pbn_servers = None
        self.pbn_servers_data = databases_data
        self.sql_pbn_servers_amazon = SQLPbn_Amazon()  # Клиенты в БД / Сервера в БД
        self.sql_amazon = SQLAmazon()  # Запросы только с Amazon DB

    def sites_articles__in__db__pbn_server(self):
        dict_articles_sites = {}
        for pbn_server_data in self.pbn_servers_data:
            ip_server = self.pbn_servers_data[pbn_server_data]['ip']
            user = self.pbn_servers_data[pbn_server_data]['login']
            password = self.pbn_servers_data[pbn_server_data]['pass']

            print(f"ip_server: {ip_server}")
            self.sql_pbn_servers = SQLSite(site_database_ip=ip_server,
                                           site_database_user=user,
                                           site_database_name='',
                                           site_database_pass=password)  # БД Игоря

            dict_articles_sites = self.sql_pbn_servers.get_all_articles_from_pbn_server(
                dict_sites=dict_articles_sites)  # Сайты в БД Игоря

        return dict_articles_sites

    def write__articles(self):
        sites_in_db_pbn_server = self.sites_articles__in__db__pbn_server()
        articles_in_db_amazon = self.sql_amazon.get_all_articles_from_amazon_db()
        list_insert = []
        qty_articles = 0
        qty_articles_add = 0
        for site_db_pbn in sites_in_db_pbn_server:
            site_name = sites_in_db_pbn_server[site_db_pbn]['site_name']

            server_id = sites_in_db_pbn_server[site_db_pbn]['server_id']
            articles = sites_in_db_pbn_server[site_db_pbn]['articles']
            site_id = sites_in_db_pbn_server[site_db_pbn]['site_id']

            if articles != "Error":
                for article in articles:

                    id_article = articles[article]['id']
                    date_create = articles[article]['date_create']
                    article_text = articles[article]['article_text']
                    article_name = articles[article]['article_name']
                    date_modified = articles[article]['date_modified']
                    article_url = articles[article]['article_url']

                    str_check = f"{id_article}{site_id}"
                    qty_articles += 1
                    if articles_in_db_amazon.get(str_check) is None:
                        list_elem = [id_article, site_id, date_create, date_modified, article_text, article_name,
                                     article_url]
                        list_insert.append(list_elem)
                        qty_articles_add += 1
                self.sql_amazon.insert_new_articles_db(list_insert=list_insert)
                list_insert = []
        print(f"qty_articles: {qty_articles}\n"
              f"qty_articles_add: {qty_articles_add}")