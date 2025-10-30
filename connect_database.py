import os
import psycopg

def _connect_with_env(url_key, host_key, db_key, user_key, pass_key, port_key, default_params):
    """Tạo kết nối PostgreSQL ưu tiên dùng URL từ biến môi trường, fallback sang thông số riêng lẻ và cuối cùng là mặc định."""
    try:
        # Ưu tiên URL đầy đủ (VD: postgres://... hoặc postgresql://...)
        full_url = os.getenv(url_key) or os.getenv("DATABASE_URL")
        if full_url:
            # Trên Render, thường cần SSL
            if "sslmode" not in full_url:
                # Không ép buộc nếu người dùng đã chỉ định, mặc định thêm require
                connector = f"{full_url}{'&' if '?' in full_url else '?'}sslmode=require"
            else:
                connector = full_url
            return psycopg.connect(connector)

        # Nếu không có URL, thử đọc các biến lẻ
        host = os.getenv(host_key, default_params.get("host"))
        database = os.getenv(db_key, default_params.get("database"))
        user = os.getenv(user_key, default_params.get("user"))
        password = os.getenv(pass_key, default_params.get("password"))
        port = os.getenv(port_key, default_params.get("port", "5432"))
        return psycopg.connect(host=host, database=database, user=user, password=password, port=port)
    except (Exception, psycopg.Error) as error:
        print(f"Lỗi khi kết nối đến Database ({db_key}):", error)
        return None

# Kết nối tới database dev
def connect_db_dev():
    return _connect_with_env(
        url_key="DEV_DATABASE_URL",
        host_key="DEV_DB_HOST",
        db_key="DEV_DB_NAME",
        user_key="DEV_DB_USER",
        pass_key="DEV_DB_PASSWORD",
        port_key="DEV_DB_PORT",
        default_params={
            "host": "10.10.10.224",
            "database": "dev_staging_sci",
            "user": "views_staging_sci",
            "password": "Assjgsklsfs@2024",
            "port": "5432",
        },
    )

# Kết nối tới database ERP_OLD
def connect_db_erp_old():
    return _connect_with_env(
        url_key="ERP_OLD_DATABASE_URL",
        host_key="ERP_OLD_DB_HOST",
        db_key="ERP_OLD_DB_NAME",
        user_key="ERP_OLD_DB_USER",
        pass_key="ERP_OLD_DB_PASSWORD",
        port_key="ERP_OLD_DB_PORT",
        default_params={
            "host": "10.10.10.154",
            "database": "sci_erp",
            "user": "reporter",
            "password": "xjsfpwzlqehd",
            "port": "5432",
        },
    )


# Kết nối tới database ERP HH
def connect_db_erp_hh():
    return _connect_with_env(
        url_key="ERP_HH_DATABASE_URL",
        host_key="ERP_HH_DB_HOST",
        db_key="ERP_HH_DB_NAME",
        user_key="ERP_HH_DB_USER",
        pass_key="ERP_HH_DB_PASSWORD",
        port_key="ERP_HH_DB_PORT",
        default_params={
            "host": "10.10.10.218",
            "database": "hh_erp",
            "user": "reporter",
            "password": "xjsfpwzlqehd",
            "port": "54321",
        },
    )

# Kết nối tới database ERP KN
def connect_db_erp_kn():
    return _connect_with_env(
        url_key="ERP_KN_DATABASE_URL",
        host_key="ERP_KN_DB_HOST",
        db_key="ERP_KN_DB_NAME",
        user_key="ERP_KN_DB_USER",
        pass_key="ERP_KN_DB_PASSWORD",
        port_key="ERP_KN_DB_PORT",
        default_params={
            "host": "10.10.10.211",
            "database": "kn_erp",
            "user": "reporter",
            "password": "xjsfpwzlqehd",
            "port": "54321",
        },
    )

# Kết nối tới database ERP PR
def connect_db_erp_pr():
    return _connect_with_env(
        url_key="ERP_PR_DATABASE_URL",
        host_key="ERP_PR_DB_HOST",
        db_key="ERP_PR_DB_NAME",
        user_key="ERP_PR_DB_USER",
        pass_key="ERP_PR_DB_PASSWORD",
        port_key="ERP_PR_DB_PORT",
        default_params={
            "host": "10.10.10.137",
            "database": "pr_erp",
            "user": "reporter",
            "password": "xjsfpwzlqehd",
            "port": "54321",
        },
    )

# Kết nối tới database ERP SCI
def connect_db_erp_sci():
    return _connect_with_env(
        url_key="ERP_SCI_DATABASE_URL",
        host_key="ERP_SCI_DB_HOST",
        db_key="ERP_SCI_DB_NAME",
        user_key="ERP_SCI_DB_USER",
        pass_key="ERP_SCI_DB_PASSWORD",
        port_key="ERP_SCI_DB_PORT",
        default_params={
            "host": "10.10.10.154",
            "database": "sci_erp",
            "user": "reporter",
            "password": "xjsfpwzlqehd",
            "port": "54321",
        },
    )


