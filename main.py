from data_scripts.res_sites import PBNSites
from data_scripts.res_pbn_servers import PbnServers
from data_scripts.res_articles import PBNArticles
from data_scripts.res_links import PBNLinks

if __name__ == '__main__':
    ObjPbnServers = PbnServers()
    ObjPbnServers.clients_main() # Записываем всех клиентов в базу из конфиг файла
    ObjPbnServers.pbn_server_names_main() # Записываем все ПБН сервера в базу

    ObjPBNSites = PBNSites()
    ObjPBNSites.write__sites() # Записываем сайты со всех ПБН серверов

    ObjPBNSites = PBNArticles()
    ObjPBNSites.write__articles() # Записываем все статьи со всех сайтов

    ObjPBNLinks = PBNLinks()
    ObjPBNLinks.main_pbn_links() # Записываем все найденные ссылки в статьях и отношения между донором/акцептором
