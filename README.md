# AntTeamPbnAnalytics
Глобальная концепция скриптов - выгрузка данных из множества баз данных различных сайтов на WordPress: статьи, названия статей, даты публикаций, парсинг текстов статей, поиск ссылок. Требуется данные для аналитика и последующей эффективности работы с PBN и ссылками.

Используются библиотеки:
pip install lxml
pip install beautifulsoup4
pip install mysql-connector-python-rf
pip install pymysql

Для работы скриптов необходимо создать в корне проекта файл config.py и в файле указать доступы к базам данных сайтов.

Для работы скрипта требуется создать таблицы в базе данных и указать к ней доступы в config.py:

"""SQL START""" 
create table clients(
id int not null auto_increment,
client_name varchar(128),
date_add date,
primary key(id)
)

create table servers(
id int not null auto_increment,
server_name varchar(64),
ip_server varchar(15),
client_id int,
date_add date,
primary key(id)
)

create table pbn_sites(
id int not null auto_increment,
site_url varchar(128),
date_create datetime,
id_server int,
primary key(id),
FOREIGN KEY (id_server)  REFERENCES servers (id)
FOREIGN KEY (id) REFERENCES links_all_domains (id)
)


create table pbn_articles(
id_row int not null auto_increment,
id_article int,
id_pbn_site int,
date_create datetime,
date_modified datetime,
text_article text,
article_name text,
article_url text,
primary key(id_row, id_article, id_pbn_site),
FOREIGN KEY (id_pbn_site)  REFERENCES pbn_sites (id)
)

create table pbn_articles_check(
id_row int not null auto_increment,
id_article int,
date_check datetime,
primary key(id_row),
FOREIGN KEY (id_article)  REFERENCES pbn_articles (id)
)

create table links_all_domains(
id int not null auto_increment,
domain_name varchar(128),
date_add date,
primary key(id)
)

create table links_all_urls(
id int not null auto_increment,
url text,
date_add date,
id_domain int,
primary key(id),
FOREIGN KEY (id_domain) REFERENCES links_all_domains (id)
)

create table links_all_anchors(
id int not null auto_increment,
anchor_value text,
date_add date,
primary key(id)
)

create table links_check_donor_acceptor(
id int not null auto_increment,
id_url_from_donor int,
id_url_to_acceptor int,
id_anchor int,
date_check date,
primary key(id),
FOREIGN KEY (id_anchor) REFERENCES links_all_anchors (id),
FOREIGN KEY (id_url_to_acceptor) REFERENCES links_all_urls (id),
FOREIGN KEY (id_url_from_donor) REFERENCES links_all_urls (id)
)

create table relation_pbn_sites_links_all_domains(
id_pbn_sites int,
id_links_all_domains int,
primary key(id_pbn_sites, id_links_all_domains)
)
"""SQL END""" 
