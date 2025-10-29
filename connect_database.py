import psycopg

# Kết nối tới database dev
def connect_db_dev():
    try:
        connection = psycopg.connect(
            host="10.10.10.224",
            database="dev_staging_sci", 
            user="views_staging_sci",
            password="Assjgsklsfs@2024",
            port="5432"
        )
        return connection 
    except (Exception, psycopg.Error) as error:
        print("Lỗi khi kết nối đến Database Dev:", error)
        return None

# Kết nối tới database ERP_OLD
def connect_db_erp_old():
    try:
        connection = psycopg.connect(
            host="10.10.10.154",
            database="sci_erp",
            user="reporter",
            password="xjsfpwzlqehd",
            port="5432"
        )
        return connection
    except (Exception, psycopg.Error) as error:
        print("Lỗi khi kết nối đến Database ERP:", error)
        return None


# Kết nối tới database ERP HH
def connect_db_erp_hh():
    try:
        connection = psycopg.connect(
            host="10.10.10.218",
            database="hh_erp",
            user="reporter",
            password="xjsfpwzlqehd",
            port="54321"
        )
        return connection
    except (Exception, psycopg.Error) as error:
        print("Lỗi khi kết nối đến Database ERP HH:", error)
        return None

# Kết nối tới database ERP KN
def connect_db_erp_kn():
    try:
        connection = psycopg.connect(
            host="10.10.10.211",
            database="kn_erp",
            user="reporter",
            password="xjsfpwzlqehd",
            port="54321"
        )
        return connection
    except (Exception, psycopg.Error) as error:
        print("Lỗi khi kết nối đến Database ERP KN:", error)
        return None

# Kết nối tới database ERP PR
def connect_db_erp_pr():
    try:
        connection = psycopg.connect(
            host="10.10.10.137",
            database="pr_erp",
            user="reporter",
            password="xjsfpwzlqehd",
            port="54321"
        )
        return connection
    except (Exception, psycopg.Error) as error:
        print("Lỗi khi kết nối đến Database ERP PR:", error)
        return None

# Kết nối tới database ERP SCI
def connect_db_erp_sci():
    try:
        connection = psycopg.connect(
            host="10.10.10.154",
            database="sci_erp",
            user="reporter",
            password="xjsfpwzlqehd",
            port="54321"
        )
        return connection
    except (Exception, psycopg.Error) as error:
        print("Lỗi khi kết nối đến Database ERP SCI:", error)
        return None


