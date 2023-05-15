"""
Все запросы, связанные с res_pbn_servers.py
"""
from database_mysql import MySQLi
from config import *


class SQLPbn_Amazon():
    def __init__(self):
        self.db_amazon = MySQLi(hostname=host_pg, username=user_pg, password=password_pg, database=database_pg)

    def get_max_id_table(self, table_name):

        max_id = self.db_amazon.fetch(f"SELECT MAX(id) FROM {table_name}")
        return max_id

    def clients_in_db(self):
        """
        Выгружаем список клиентов из базы данных в формате словаря
        :return: {'client_name':
        {'id': '1', 'client_name': 'site.ru'} }
        """

        max_id_table = self.get_max_id_table(table_name='clients')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 100000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "client_name "
                                                        "FROM clients AS c "
                                                        "WHERE c.id BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']
                for data_table in data_table_mysql:
                    id = data_table[0]
                    client_name = data_table[1]

                    data_table_dict[client_name] = {
                        'id': id,
                        'client_name': client_name
                    }
            return data_table_dict
        else:
            return {}

    def insert_new_clients_db(self, list_insert):
        if len(list_insert) > 0:
            self.db_amazon.commit("INSERT clients(client_name, date_add) VALUES (%s, %s)",
                                  list_insert, type_commit="many")

    def servers_in_db(self):
        """
        Выгружаем список серверов из базы данных в формате словаря
        """
        max_id_table = self.get_max_id_table(table_name='servers')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 100000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "server_name, "
                                                        "ip_server, "
                                                        "client_id "
                                                        "FROM servers AS s "
                                                        "WHERE s.id BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']
                for data_table in data_table_mysql:
                    id = data_table[0]
                    server_name = data_table[1]
                    ip_server = data_table[2]
                    client_id = data_table[3]

                    data_table_dict[server_name] = {
                        'id': id,
                        'server_name': server_name,
                        'ip_server': ip_server,
                        'client_id': client_id
                    }
            return data_table_dict
        else:
            return {}

    def insert_new_pbn_servers_db(self, list_insert):
        if len(list_insert) > 0:
            self.db_amazon.commit("INSERT servers(server_name, ip_server, client_id, date_add) VALUES (%s, %s, %s, %s)",
                                  list_insert, type_commit="many")

#
# ObSQLPbn_Amazon = SQLPbn_Amazon()
# a = ObSQLPbn_Amazon.clients_in_db()
# print(a)
