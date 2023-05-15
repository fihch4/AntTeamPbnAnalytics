import datetime

from database_mysql import MySQLi
from config import *


class SQLLinks():
    def __init__(self):
        self.db_amazon = MySQLi(hostname=host_pg,
                                username=user_pg,
                                password=password_pg,
                                database=database_pg)

    def get_max_id_table(self, table_name, column_name):
        max_id = self.db_amazon.fetch(f"SELECT MAX({column_name}) FROM {table_name}")
        return max_id

    def get_max_id_table_donor_acceptor_date_now(self):
        datetime_now = datetime.datetime.now().date()
        max_id = self.db_amazon.fetch(f"SELECT MAX(id) FROM links_check_donor_acceptor WHERE date_check = '{datetime_now}'")
        return max_id

    def get_article_texts_from_db(self):
        max_id_table = self.get_max_id_table(table_name='pbn_articles', column_name='id_row')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id_row, "
                                                        "text_article, "
                                                        "article_url, "
                                                        "ps.site_url "
                                                        "FROM pbn_articles AS a "
                                                        "INNER JOIN pbn_sites AS ps ON ps.id = a.id_pbn_site "
                                                        "WHERE a.id_row BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']
                for data_table in data_table_mysql:
                    id_row = data_table[0]
                    text_article = data_table[1]
                    article_url = data_table[2]
                    site_url = data_table[3]

                    data_table_dict[id_row] = {
                        'id_row': id_row,
                        'text_article': text_article,
                        'article_url': article_url,
                        'site_url': site_url
                    }
            return data_table_dict
        else:
            return {}

    def get_urls_from_db(self):
        max_id_table = self.get_max_id_table(table_name='links_all_urls', column_name='id')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "url "
                                                        "FROM links_all_urls AS la "
                                                        "WHERE la.id BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']

                for data_table in data_table_mysql:
                    id = data_table[0]
                    url = data_table[1]

                    data_table_dict[url] = {
                        'id': id,
                        'url': url
                    }
            return data_table_dict
        else:
            return {}

    def insert_new_urls(self, list_insert):
        if len(list_insert) > 0:
                self.db_amazon.commit("INSERT links_all_urls(url, date_add, id_domain) VALUES (%s, %s, %s)",
                                      list_insert, type_commit="many")

    def get_domains_from_db(self):
        max_id_table = self.get_max_id_table(table_name='links_all_domains', column_name='id')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "domain_name "
                                                        "FROM links_all_domains AS lad "
                                                        "WHERE lad.id BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']

                for data_table in data_table_mysql:
                    id = data_table[0]
                    domain_name = data_table[1]

                    data_table_dict[domain_name] = {
                        'id': id,
                        'domain_name': domain_name
                    }
            return data_table_dict
        else:
            return {}

    def get_pbn_domains_from_db(self):
        max_id_table = self.get_max_id_table(table_name='pbn_sites', column_name='id')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "site_url "
                                                        "FROM pbn_sites AS ps "
                                                        "WHERE ps.id BETWEEN %s AND %s", i, max_now)
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

    def insert_new_domains(self, list_insert):
        if len(list_insert) > 0:
                self.db_amazon.commit("INSERT links_all_domains(domain_name, date_add) VALUES (%s, %s)",
                                      list_insert, type_commit="many")

    def get_anchors_from_db(self):
        max_id_table = self.get_max_id_table(table_name='links_all_anchors', column_name='id')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "anchor_value "
                                                        "FROM links_all_anchors AS laa "
                                                        "WHERE laa.id BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']

                for data_table in data_table_mysql:
                    id = data_table[0]
                    anchor_value = data_table[1]

                    data_table_dict[anchor_value] = {
                        'id': id,
                        'anchor_value': anchor_value
                    }
            return data_table_dict
        else:
            return {}

    def insert_new_anchors(self, list_insert):
        if len(list_insert) > 0:
                self.db_amazon.commit("INSERT links_all_anchors(anchor_value, date_add) VALUES (%s, %s)",
                                      list_insert, type_commit="many")

    def get_data_donor_acceptor(self):
        max_id_table = self.get_max_id_table_donor_acceptor_date_now()

        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            datetime_now = datetime.datetime.now().date()

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id, "
                                                        "id_url_from_donor, "
                                                        "id_url_to_acceptor "
                                                        "FROM links_check_donor_acceptor AS lda "
                                                        "WHERE lda.date_check = %s AND lda.id BETWEEN %s AND %s", datetime_now, i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']

                for data_table in data_table_mysql:
                    id = data_table[0]
                    id_url_from_donor = data_table[1]
                    id_url_to_acceptor = data_table[2]
                    str_check = f"{id_url_from_donor}{id_url_to_acceptor}"

                    data_table_dict[str_check] = {
                        'id': id,
                        'id_url_from_donor': id_url_from_donor,
                        'id_url_to_acceptor': id_url_to_acceptor
                    }
            return data_table_dict
        else:
            return {}

    def insert_donor_acceptor(self, list_insert):
        if len(list_insert) > 0:
                self.db_amazon.commit("INSERT links_check_donor_acceptor(id_url_from_donor, id_url_to_acceptor, id_anchor, date_check) "
                                      "VALUES (%s, %s, %s, %s)",
                                      list_insert, type_commit="many")

    def get_relation_domains_db(self):
        max_id_table = self.get_max_id_table(table_name='links_all_domains', column_name='id')
        if len(max_id_table['rows'][0]) >= 1 and max_id_table['rows'][0][0] is not None:
            max_id_table = max_id_table['rows'][0][0]
            max_id_table = int(max_id_table)

            step_range = 1000
            now_index = 0
            data_table_dict = {}
            max_now = now_index + step_range
            for i in range(now_index, max_id_table, step_range):
                data_table_mysql = self.db_amazon.fetch("SELECT "
                                                        "id_pbn_sites, "
                                                        "id_links_all_domains "
                                                        "FROM relation_pbn_sites_links_all_domains AS ps "
                                                        "WHERE ps.id_links_all_domains BETWEEN %s AND %s", i, max_now)
                max_now = i + step_range * 2
                data_table_mysql = data_table_mysql['rows']

                for data_table in data_table_mysql:
                    id_pbn_sites = data_table[0]
                    id_links_all_domains = data_table[1]
                    str_check = f"{id_pbn_sites}{id_links_all_domains}"

                    data_table_dict[str_check] = {
                        'id_pbn_sites': id_pbn_sites,
                        'id_links_all_domains': id_links_all_domains
                    }
            return data_table_dict
        else:
            return {}


    def insert_relation_domains(self, list_insert):
        if len(list_insert) > 0:
            self.db_amazon.commit(
                "INSERT relation_pbn_sites_links_all_domains(id_pbn_sites, id_links_all_domains) "
                "VALUES (%s, %s)",
                list_insert, type_commit="many")
