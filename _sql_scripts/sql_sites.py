"""
Все запросы, связанные с res_sites.py
"""
from database_mysql import MySQLi
from database_pymysql import PyMySQLi
from config import *


class SQLSite():
    def __init__(self, site_database_name, site_database_pass, site_database_ip, site_database_user):
        self.db_pymysql_site_database = None
        self.db_amazon = MySQLi(hostname=host_pg,
                                username=user_pg,
                                password=password_pg,
                                database=database_pg)

        self.pbn_servers_data = databases_data
        self.site_database_ip = site_database_ip
        self.database_name = site_database_name
        self.site_database_user = site_database_user
        self.site_database_pass = site_database_pass
        self.db_pymysql_site = PyMySQLi(ip=self.site_database_ip,
                                        username=self.site_database_user,
                                        password=self.site_database_pass,
                                        database=self.database_name)

    def get_date_create_user(self, database_name):
        self.db_pymysql_site_database = PyMySQLi(ip=self.site_database_ip,
                                                 username=self.site_database_user,
                                                 password=self.site_database_pass,
                                                 database=database_name)
        date_create = self.db_pymysql_site_database.fetch(
            "select user_registered from wp_users order by user_registered DESC LIMIT 1")
        date_create = date_create['rows']
        if len(date_create) > 0:
            return date_create[0][0]
        else:
            return "Error"

    def get_site_name_db(self, database_name):
        self.db_pymysql_site_database = PyMySQLi(ip=self.site_database_ip,
                                                 username=self.site_database_user,
                                                 password=self.site_database_pass,
                                                 database=database_name)
        date_create = self.db_pymysql_site_database.fetch(
            "select option_value from wp_options where option_name = 'siteurl'")
        date_create = date_create['rows']
        if len(date_create) > 0:
            site_name = date_create[0][0]
            if 'http://' in site_name:
                site_name = site_name.replace('http://', '')
            elif 'https://' in site_name:
                site_name = site_name.replace('https://', '')

            if 'www.' in site_name:
                site_name = site_name.replace('www.', '')

            return site_name
        else:
            return "Error"

    def get_server_id(self):
        server_id = self.db_amazon.fetch(f"SELECT id FROM servers WHERE ip_server = '{self.site_database_ip}'")
        server_id = server_id['rows']
        if len(server_id) > 0:
            return server_id[0][0]
        else:
            return "Error"

    def get_all_sites_from_pbn_server(self, dict_sites):
        server_id = self.get_server_id()
        sites_db_server = self.db_pymysql_site.fetch("SHOW DATABASES")

        if len(sites_db_server['rows']) > 0:
            sites_db_server = sites_db_server['rows']
            for site in sites_db_server:
                site_db_name = site[0]
                if site_db_name != 'information_schema':
                    # site_name = site_db_name.split('pbn_')
                    # site_name = site_name[1].replace('_', '.')
                    site_name = self.get_site_name_db(database_name=site_db_name)
                    dict_sites[site_db_name] = {'ip_server': self.site_database_ip,
                                                'site_name': site_name,
                                                'site_db_name': site_db_name,
                                                'date_create': self.get_date_create_user(database_name=site_db_name),
                                                'server_id': server_id
                                                }
        return dict_sites


class SQLAmazon():
    def __init__(self):
        self.db_amazon = MySQLi(hostname=host_pg,
                                username=user_pg,
                                password=password_pg,
                                database=database_pg)

    def get_max_id_table(self, table_name):

        max_id = self.db_amazon.fetch(f"SELECT MAX(id) FROM {table_name}")
        return max_id

    def get_all_sites_from_amazon_db(self):
        max_id_table = self.get_max_id_table(table_name='pbn_sites')
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
                                                        "site_url "
                                                        "FROM pbn_sites AS s "
                                                        "WHERE s.id BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']
                for data_table in data_table_mysql:
                    id = data_table[0]
                    site_url = data_table[1]

                    data_table_dict[site_url] = {
                        'id': id,
                        'site_url': site_url
                    }
            return data_table_dict
        else:
            return {}

    def insert_new_sites_db(self, list_insert):
        if len(list_insert) > 0:
            self.db_amazon.commit("INSERT pbn_sites(site_url, date_create, id_server) VALUES (%s, %s, %s)",
                                  list_insert, type_commit="many")
