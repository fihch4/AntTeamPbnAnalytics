from _sql_scripts.sql_sites import *
from _sql_scripts.sql_pbn_servers import *


class PBNSites():
    """
    Скрипт отвечает за:
    1. Опрашивает базу данных на амазоне. Забирает из нее сайты
    2. Опрашивает все базы данных PBN серверов. Забирает из них сайты
    3. Записывает в БД амазона все отсутствующие сайты.
    4. #TODO: Нужно ли удаление из БД амазона сайтов, которые удалены с сервера?
    """

    def __init__(self):
        self.sql_pbn_servers = None
        self.pbn_servers_data = databases_data
        self.sql_pbn_servers_amazon = SQLPbn_Amazon()  # Клиенты в БД / Сервера в БД
        self.sql_amazon = SQLAmazon()  # Запросы только с Amazon DB

    def sites__in__db__pbn_server(self):
        dict_sites = {}
        for pbn_server_data in self.pbn_servers_data:
            ip_server = self.pbn_servers_data[pbn_server_data]['ip']
            user = self.pbn_servers_data[pbn_server_data]['login']
            password = self.pbn_servers_data[pbn_server_data]['pass']

            print(f"ip_server: {ip_server}")
            self.sql_pbn_servers = SQLSite(site_database_ip=ip_server,
                                           site_database_user=user,
                                           site_database_name='',
                                           site_database_pass=password)  # БД Игоря

            dict_sites = self.sql_pbn_servers.get_all_sites_from_pbn_server(dict_sites=dict_sites)  # Сайты в БД Игоря
        return dict_sites

    def write__sites(self):
        sites_in_db_pbn_server = self.sites__in__db__pbn_server()
        sites_in_db_amazon = self.sql_amazon.get_all_sites_from_amazon_db()
        list_insert = []

        for site_db_pbn in sites_in_db_pbn_server:
            site_name = sites_in_db_pbn_server[site_db_pbn]['site_name']
            date_create = sites_in_db_pbn_server[site_db_pbn]['date_create']
            server_id = sites_in_db_pbn_server[site_db_pbn]['server_id']
            if sites_in_db_amazon.get(site_name) is None:
                list_elem = [site_name, date_create, server_id]
                list_insert.append(list_elem)

        self.sql_amazon.insert_new_sites_db(list_insert=list_insert)


if __name__ == '__main__':
    ObjPBNSites = PBNSites()
    ObjPBNSites.write__sites()
