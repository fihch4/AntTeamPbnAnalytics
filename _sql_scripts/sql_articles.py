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

    def get_articles(self, database_name, site_url):
        self.db_pymysql_site_database = PyMySQLi(ip=self.site_database_ip,
                                                 username=self.site_database_user,
                                                 password=self.site_database_pass,
                                                 database=database_name)
        articles = self.db_pymysql_site_database.fetch(
            "SELECT wp.id, wp.post_date, wp.post_content, wp.post_title, wp.post_modified , wt.slug, wp.post_name "
            "FROM wp_posts AS wp "
            "INNER JOIN wp_term_relationships AS wtr ON wtr.object_id = wp.id "
            "INNER JOIN wp_terms AS wt ON wt.term_id = wtr.term_taxonomy_id "
            "WHERE wp.post_type = 'post' "
            "AND wp.post_status = 'publish' OR wp.post_status = 'future'")

        articles = articles['rows']
        articles_dict = {}
        for article in articles:
            id = article[0]
            date_create = article[1]
            article_text = article[2]
            article_name = article[3]
            date_modified = article[4]
            slug = article[5]
            article_short_url = article[6]
            article_url = f"{site_url}/{slug}/{article_short_url}"
            articles_dict[id] = {
                'id': id,
                'date_create': date_create,
                'article_text': article_text,
                'article_name': article_name,
                'date_modified': date_modified,
                'article_url': article_url

            }
        return articles_dict

    def get_server_id(self):
        server_id = self.db_amazon.fetch(f"SELECT id FROM servers WHERE ip_server = '{self.site_database_ip}'")
        server_id = server_id['rows']
        if len(server_id) > 0:
            return server_id[0][0]
        else:
            return "Error"

    def get_site_id(self, site_name, server_id):
        site_id = self.db_amazon.fetch(
            f"SELECT id FROM pbn_sites WHERE id_server = '{server_id}' AND site_url = '{site_name}'")
        site_id = site_id['rows']
        if len(site_id) > 0:
            return site_id[0][0]
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

    def get_all_articles_from_pbn_server(self, dict_sites):
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
                    site_id = self.get_site_id(site_name=site_name, server_id=server_id)
                    if site_id != "Error":
                        dict_sites[site_db_name] = {'ip_server': self.site_database_ip,
                                                    'site_name': site_name,
                                                    'site_db_name': site_db_name,
                                                    'server_id': server_id,
                                                    'articles': self.get_articles(database_name=site_db_name,
                                                                                  site_url=site_name),
                                                    'site_id': site_id
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

    def get_max_id_table_articles(self, table_name):

        max_id = self.db_amazon.fetch(f"SELECT MAX(id_row) FROM {table_name}")
        return max_id

    def get_all_articles_from_amazon_db(self):
        max_id_table = self.get_max_id_table_articles(table_name='pbn_articles')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 100000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id_article, "
                                                        "id_pbn_site "
                                                        "FROM pbn_articles AS s "
                                                        "WHERE s.id_row BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']
                for data_table in data_table_mysql:
                    id_article = data_table[0]
                    id_pbn_site = data_table[1]

                    str_check = f"{id_article}{id_pbn_site}"
                    data_table_dict[str_check] = {
                        'id_article': id_article,
                        'id_pbn_site': id_pbn_site
                    }
            return data_table_dict
        else:
            return {}

    def insert_new_articles_db(self, list_insert):
        if len(list_insert) > 0:
            self.db_amazon.commit("INSERT pbn_articles(id_article, id_pbn_site, date_create, "
                                  "date_modified, text_article, article_name, article_url) "
                                  "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                  list_insert, type_commit="many")