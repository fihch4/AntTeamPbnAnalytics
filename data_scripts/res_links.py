from _sql_scripts.sql_links import *
from bs4 import BeautifulSoup
from urllib.parse import urlparse

"""
pip install lxml
pip install beautifulsoup4
"""


class PBNLinks():

    def __init__(self):
        self.sql_amazon = SQLLinks()  # Запросы только с Amazon DB

    def get_texts(self):
        texts_dict = self.sql_amazon.get_article_texts_from_db()
        return texts_dict

    def domain_url_from_url(self, url):
        domain = urlparse(url).netloc
        if 'www.' in domain:
            domain = domain.replace('www.', '')

        domain = domain.lower()
        return domain

    def write_domains(self, texts):
        domains_list = []
        temp_list = []
        domains_db = self.sql_amazon.get_domains_from_db()

        for text in texts:
            soup = BeautifulSoup(texts[text]['text_article'], "lxml")
            for link in soup.findAll('a'):
                href = link.get('href')
                if href.startswith('http://') or href.startswith('https://'):
                    link_url = str(href).lower()
                    domain_url = self.domain_url_from_url(link_url)
                    if domains_db.get(domain_url) is None:
                        list_elem = [domain_url, datetime.datetime.now().date()]
                        if domain_url not in temp_list:
                            domains_list.append(list_elem)
                            temp_list.append(domain_url)

        self.sql_amazon.insert_new_domains(list_insert=domains_list)

    def write_urls(self, texts):
        urls_list = []
        temp_list = []
        urls_db = self.sql_amazon.get_urls_from_db()
        domains_db = self.sql_amazon.get_domains_from_db()

        for text in texts:
            soup = BeautifulSoup(texts[text]['text_article'], "lxml")
            for link in soup.findAll('a'):
                href = link.get('href')
                if href.startswith('http://') or href.startswith('https://'):
                    link_url = str(href).lower()
                    domain_url = self.domain_url_from_url(link_url)
                    if domains_db.get(domain_url) is not None:
                        id_domain = domains_db[domain_url]['id']
                        if urls_db.get(link_url) is None:
                            list_elem = [link_url, datetime.datetime.now().date(), id_domain]
                            if link_url not in temp_list:
                                urls_list.append(list_elem)
                                temp_list.append(link_url)

        self.sql_amazon.insert_new_urls(urls_list)

    def write_urls_pbn(self):
        urls_from_db = self.sql_amazon.get_urls_from_db()  # Все URL в базе
        urls_from_articles_db = self.sql_amazon.get_article_texts_from_db()  # URL адреса из статей (доноров)
        domains_db = self.sql_amazon.get_domains_from_db()

        urls_insert = []
        temp_list = []

        for id_row in urls_from_articles_db:
            article_url = urls_from_articles_db[id_row]['article_url']
            protocol_article_url = 'https://' + str(article_url) + '/'
            if urls_from_db.get(protocol_article_url) is None and protocol_article_url not in temp_list:
                temp_list.append(protocol_article_url)
                domain_url = self.domain_url_from_url(protocol_article_url)
                if domains_db.get(domain_url) is not None:
                    id_domain = domains_db[domain_url]['id']
                    list_elem = [protocol_article_url, datetime.datetime.now().date(), id_domain]
                    urls_insert.append(list_elem)
        self.sql_amazon.insert_new_urls(urls_insert)

        pass

    def write_all_pbn_domains(self):
        """
        Записываем дропы в общую таблицу доменов
        :return:
        """
        list_insert = []
        temp_list = []
        domains_db = self.sql_amazon.get_pbn_domains_from_db()  # Дропы из базы (Условно в базе Игоря)
        pbn_domains_db = self.sql_amazon.get_domains_from_db()  # Все домены в базе

        for domain_pbn in domains_db:
            if pbn_domains_db.get(domain_pbn) is None:
                if domain_pbn not in temp_list:
                    list_elem = [domain_pbn, datetime.datetime.now().date()]
                    list_insert.append(list_elem)
        self.sql_amazon.insert_new_domains(list_insert=list_insert)

    def write_anchors(self, texts):
        list_insert = []
        temp_list = []
        anchors_in_db = self.sql_amazon.get_anchors_from_db()

        for text in texts:
            soup = BeautifulSoup(texts[text]['text_article'], "lxml")
            for link in soup.findAll('a'):
                href = link.get('href')
                if href.startswith('http://') or href.startswith('https://'):
                    anchor_value = link.string
                    anchor_value = str(anchor_value).lower()
                    if anchors_in_db.get(anchor_value) is None and anchor_value not in temp_list:
                        temp_list.append(anchor_value)
                        list_elem = [anchor_value, datetime.datetime.now().date()]
                        list_insert.append(list_elem)
        self.sql_amazon.insert_new_anchors(list_insert=list_insert)

    def write_donor_acceptor(self, texts):
        anchors_in_db = self.sql_amazon.get_anchors_from_db()
        urls_from_db = self.sql_amazon.get_urls_from_db()  # Все URL в базе
        donor_acceptor_in_db = self.sql_amazon.get_data_donor_acceptor()  # Данные из donor_acceptor на дату обращения

        urls_insert = []
        temp_list = []

        for id_row in texts:
            article_url = texts[id_row]['article_url']
            donor_protocol_article_url = 'https://' + str(article_url) + '/'
            id_donor_url = urls_from_db[donor_protocol_article_url]['id']

            soup = BeautifulSoup(texts[id_row]['text_article'], "lxml")
            for link in soup.findAll('a'):
                href = link.get('href')
                if href.startswith('http://') or href.startswith('https://'):
                    acceptor_link_url = str(href).lower()
                    id_acceptor_url = urls_from_db[acceptor_link_url]['id']

                    anchor_value = link.string
                    anchor_value = str(anchor_value).lower()
                    id_anchor = anchors_in_db[anchor_value]['id']

                    str_check = f"{id_donor_url}{id_acceptor_url}"
                    if donor_acceptor_in_db.get(str_check) is None:
                        if str_check not in temp_list:
                            list_elem = [id_donor_url, id_acceptor_url, id_anchor, datetime.datetime.now().date()]
                            urls_insert.append(list_elem)
                            temp_list.append(str_check)
        self.sql_amazon.insert_donor_acceptor(urls_insert)

    def relation_pbn_sites_links_all_domains(self):
        pbn_sites_db = self.sql_amazon.get_pbn_domains_from_db() # Домены клиентов в pbn_sites
        all_domains_db = self.sql_amazon.get_domains_from_db() # Все домены в базе
        data_relation_from_db = self.sql_amazon.get_relation_domains_db() # Таблица отношений в бзе
        """
        Перебираем домены из базы клиентов, собираем их ID. Смотрим, какой ID доменов в общей базе доменов,
        записываем связи в таблицу отношений.
        
        """
        list_insert = []
        for site_url in pbn_sites_db:
            domain_name = site_url # Название доменов в базе клиентов pbn_sites
            id_domain_name_pbn = pbn_sites_db[site_url]['id'] # ID домена в базе клиентов pbn_sites
            id_domain_from_all_domain = all_domains_db[domain_name]['id'] # ID домена в links_all_domains

            str_check = f"{id_domain_name_pbn}{id_domain_from_all_domain}"
            if data_relation_from_db.get(str_check) is None:
                list_elem = [id_domain_name_pbn, id_domain_from_all_domain]
                list_insert.append(list_elem)

        self.sql_amazon.insert_relation_domains(list_insert=list_insert)

    def main_pbn_links(self):
        texts_db = self.get_texts()
        self.write_domains(texts=texts_db)  # Записываем домены в БД
        self.write_urls(texts=texts_db)  # Записываем URL в БД
        self.write_all_pbn_domains()  # Записываем дропы из базы дропов
        self.write_anchors(texts=texts_db)  # Записываем анкоры в БД
        self.write_urls_pbn()  # Записываем URL адреса статей (доноров) в БД
        self.write_donor_acceptor(texts=texts_db)  # Записываем связи (проверки) доноров-акцепторов в БД
        self.relation_pbn_sites_links_all_domains() # Записываем связи доменов ПБН клиентов с общей таблицей доменов


if __name__ == '__main__':
    ObjPBNLinks = PBNLinks()
    ObjPBNLinks.main_pbn_links()
