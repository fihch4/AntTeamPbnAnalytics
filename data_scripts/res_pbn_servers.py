import datetime
from _sql_scripts.sql_pbn_servers import *


class PbnServers():
    """
    Сущность ПБН сервера. Сущность может делать:
    -Добавляются новые PBN серверы VS date
    -Удаляются старые VS date
    -У каждого PBN сервера добавляются новые сайты VS date
    -У каждого PBN сервера удаляются старые сайты VS date
    -Получение названий клиентов
    -Добавление новых клиентов
    -Удаление старых клиентов
    -Матчинг PBN серверов с таблицей клиентов
    """

    def __init__(self):
        self.pbn_servers_data = databases_data
        self.sql_pbn_servers_amazon = SQLPbn_Amazon()

    def get_clients_from_config(self):
        clients_list = []
        for pbn_server_name in self.pbn_servers_data:
            if '_ver' in pbn_server_name:
                client_name = pbn_server_name.split('_')
                client_name = client_name[0]
            else:
                client_name = pbn_server_name

            if client_name not in clients_list:
                clients_list.append(client_name)
        return clients_list

    def insert_new_clients(self, clients_config, clients_db):
        list_insert = []
        for client_config in clients_config:
            if clients_db.get(client_config) is None:
                list_elem = [client_config, datetime.datetime.now().date()]
                list_insert.append(list_elem)
        self.sql_pbn_servers_amazon.insert_new_clients_db(list_insert=list_insert)

    def clients_main(self):
        """
        Функция опрашивает конфиг-данные по серверам, определяет имена клиентов и формирует общий список клиентов.
        Обращается к базе данных, забирает все имена клиентов. Если найдено новое имя клиента, записывает его в БД.
        :return: отсутствует возврат данных.
        #TODO: Добавить удаление клиентов из БД, если какой-то клиент пропал из конфига. (нужно ли это)
        """
        clients_config = self.get_clients_from_config()
        clients_database = self.sql_pbn_servers_amazon.clients_in_db()
        self.insert_new_clients(clients_config=clients_config, clients_db=clients_database)

    def get_pbn_servers_names_from_config(self):
        dict_servers = {}
        for pbn_server_name in self.pbn_servers_data:
            if '_ver' in pbn_server_name:
                server_name = pbn_server_name.split('_')
                server_name_dict = server_name[1]
                client_name = server_name[0]
            else:
                server_name_dict = pbn_server_name
                client_name = ''

            dict_servers[pbn_server_name] = {'server_name': pbn_server_name,
                                             'client_name': client_name,
                                             'ip_server': self.pbn_servers_data[pbn_server_name]['ip']}

        return dict_servers

    def insert_new_pbn_servers_name(self, servers_names_dict, servers_db):
        list_insert = []
        clients_database = self.sql_pbn_servers_amazon.clients_in_db()

        for data_server_dict in servers_names_dict:
            server_name = servers_names_dict[data_server_dict]['server_name']
            client_name = servers_names_dict[data_server_dict]['client_name']
            if clients_database.get(client_name) is None:
                client_id = ''
            else:
                client_id = clients_database[client_name]['id']
            ip_server = servers_names_dict[data_server_dict]['ip_server']

            if servers_db.get(server_name) is None:
                list_elem = [server_name, ip_server, client_id, datetime.datetime.now().date()]
                list_insert.append(list_elem)
        self.sql_pbn_servers_amazon.insert_new_pbn_servers_db(list_insert=list_insert)

    def pbn_server_names_main(self):
        """
        Функция опрашивает конфиг-данные по серверам, определяет имена серверов, и формирует общий список серверов, ip, id клиентов.
        Обращается к базе данных, забирает все имена серверов. Если найдено новое имя сервера, записывает его в БД.
        :return: отсутствует возврат данных.
        #TODO: Добавить удаление серверов из БД, если какой-то сервер пропал из конфига. (нужно ли это)
        """
        pbn_servers_config_dict = self.get_pbn_servers_names_from_config()
        pbn_servers_db = self.sql_pbn_servers_amazon.servers_in_db()
        self.insert_new_pbn_servers_name(servers_names_dict=pbn_servers_config_dict, servers_db=pbn_servers_db)




