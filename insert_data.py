import pandas as pd
import psycopg2
from psycopg2 import sql
from connect_database import connect_db_dev, connect_db_erp_kn, connect_db_erp_pr, connect_db_erp_hh, connect_db_erp_old, connect_db_erp_sci
import os
from psycopg2.extras import execute_values
import numpy as np

# Hàm kết nối đến database DEV
def connect_db():
    return connect_db_dev()

def truncate_target_table_dev(connection, target_table):
    """Xóa toàn bộ dữ liệu trong bảng target ở DB DEV trong 1 transaction."""
    with connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {target_table}")
    connection.commit()

def stream_fetch_data_from_kn(source_connection, source_table, where_clause=None, fetch_size=5000):
    test_cursor = source_connection.cursor()
    try:
        count_sql = f"SELECT COUNT(*) FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
        test_cursor.execute(count_sql)
        count = test_cursor.fetchone()[0]
        print(f"Tìm thấy {count} records trong {source_table} (KN){' với điều kiện: ' + where_clause if where_clause else ''}")
    except Exception as e:
        print(f"Lỗi khi test query KN: {e}")
        raise
    finally:
        test_cursor.close()
    cursor = source_connection.cursor()
    select_sql = f"SELECT * FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
    cursor.execute(select_sql)
    return cursor

def stream_fetch_data_from_pr(source_connection, source_table, where_clause=None, fetch_size=5000):
    test_cursor = source_connection.cursor()
    try:
        count_sql = f"SELECT COUNT(*) FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
        test_cursor.execute(count_sql)
        count = test_cursor.fetchone()[0]
        print(f"Tìm thấy {count} records trong {source_table} (PR){' với điều kiện: ' + where_clause if where_clause else ''}")
    except Exception as e:
        print(f"Lỗi khi test query PR: {e}")
        raise
    finally:
        test_cursor.close()
    cursor = source_connection.cursor()
    select_sql = f"SELECT * FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
    cursor.execute(select_sql)
    return cursor

def stream_fetch_data_from_hh(source_connection, source_table, where_clause=None, fetch_size=5000):
    test_cursor = source_connection.cursor()
    try:
        count_sql = f"SELECT COUNT(*) FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
        test_cursor.execute(count_sql)
        count = test_cursor.fetchone()[0]
        print(f"Tìm thấy {count} records trong {source_table} (HH){' với điều kiện: ' + where_clause if where_clause else ''}")
    except Exception as e:
        print(f"Lỗi khi test query HH: {e}")
        raise
    finally:
        test_cursor.close()
    cursor = source_connection.cursor()
    select_sql = f"SELECT * FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
    cursor.execute(select_sql)
    return cursor

def stream_fetch_data_from_old(source_connection, source_table, where_clause=None, fetch_size=5000):
    test_cursor = source_connection.cursor()
    try:
        count_sql = f"SELECT COUNT(*) FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
        test_cursor.execute(count_sql)
        count = test_cursor.fetchone()[0]
        print(f"Tìm thấy {count} records trong {source_table} (OLD){' với điều kiện: ' + where_clause if where_clause else ''}")
    except Exception as e:
        print(f"Lỗi khi test query OLD: {e}")
        raise
    finally:
        test_cursor.close()
    cursor = source_connection.cursor()
    select_sql = f"SELECT * FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
    cursor.execute(select_sql)
    return cursor

def stream_fetch_data_from_sci(source_connection, source_table, where_clause=None, fetch_size=5000):
    test_cursor = source_connection.cursor()
    try:
        count_sql = f"SELECT COUNT(*) FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
        test_cursor.execute(count_sql)
        count = test_cursor.fetchone()[0]
        print(f"Tìm thấy {count} records trong {source_table} (SCI){' với điều kiện: ' + where_clause if where_clause else ''}")
    except Exception as e:
        print(f"Lỗi khi test query SCI: {e}")
        raise
    finally:
        test_cursor.close()
    cursor = source_connection.cursor()
    select_sql = f"SELECT * FROM {source_table}" + (f" WHERE {where_clause}" if where_clause else "")
    cursor.execute(select_sql)
    return cursor

def copy_kn_data_to_dev(dev_connection, kn_connection, source_table, target_table, brand_etl=1, where_clause=None, batch_size=5000):
    """
    Sao chép dữ liệu từ ERP KN sang DEV theo lô.
    Thêm cột brand_etl cho tất cả records.
    """
    source_cursor = stream_fetch_data_from_kn(kn_connection, source_table, where_clause=where_clause, fetch_size=batch_size)

    if source_cursor.description is None:
        raise RuntimeError(f"Không thể lấy dữ liệu từ ERP KN - cursor không hợp lệ")

    # Lấy cột từ bảng nguồn và đích
    source_columns = [desc.name for desc in source_cursor.description]
    target_columns = get_target_table_columns(dev_connection, target_table)
    
    # Lọc chỉ những cột có trong cả hai bảng
    common_columns = filter_common_columns(source_columns, target_columns)
    
    # Thêm brand_etl nếu có trong bảng đích
    if 'brand_etl' in target_columns:
        common_columns.append('brand_etl')
    
    print(f"Các cột sẽ insert từ KN: {common_columns}")

    insert_query = build_insert_statement_for_target(target_table, common_columns)

    rows_buffer = []
    total_inserted = 0

    with dev_connection.cursor() as dev_cursor:
        for row in source_cursor:
            # Chỉ lấy giá trị của các cột có trong cả hai bảng
            filtered_row = []
            for i, col_name in enumerate(source_columns):
                if col_name in common_columns:
                    filtered_row.append(row[i])
            
            # Thêm brand_etl nếu cần
            if 'brand_etl' in common_columns:
                filtered_row.append(brand_etl)
                
            rows_buffer.append(tuple(filtered_row))
            if len(rows_buffer) >= batch_size:
                execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
                total_inserted += len(rows_buffer)
                rows_buffer.clear()
        if rows_buffer:
            execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
            total_inserted += len(rows_buffer)

    dev_connection.commit()
    return total_inserted

def copy_pr_data_to_dev(dev_connection, pr_connection, source_table, target_table, brand_etl=3, where_clause=None, batch_size=5000):
    """
    Sao chép dữ liệu từ ERP PR sang DEV theo lô.
    Thêm cột brand_etl cho tất cả records.
    """
    source_cursor = stream_fetch_data_from_pr(pr_connection, source_table, where_clause=where_clause, fetch_size=batch_size)

    if source_cursor.description is None:
        raise RuntimeError(f"Không thể lấy dữ liệu từ ERP PR - cursor không hợp lệ")

    # Lấy cột từ bảng nguồn và đích
    source_columns = [desc.name for desc in source_cursor.description]
    target_columns = get_target_table_columns(dev_connection, target_table)
    
    # Lọc chỉ những cột có trong cả hai bảng
    common_columns = filter_common_columns(source_columns, target_columns)
    
    # Thêm brand_etl nếu có trong bảng đích
    if 'brand_etl' in target_columns:
        common_columns.append('brand_etl')
    
    print(f"Các cột sẽ insert từ PR: {common_columns}")

    insert_query = build_insert_statement_for_target(target_table, common_columns)

    rows_buffer = []
    total_inserted = 0

    with dev_connection.cursor() as dev_cursor:
        for row in source_cursor:
            # Chỉ lấy giá trị của các cột có trong cả hai bảng
            filtered_row = []
            for i, col_name in enumerate(source_columns):
                if col_name in common_columns:
                    filtered_row.append(row[i])
            
            # Thêm brand_etl nếu cần
            if 'brand_etl' in common_columns:
                filtered_row.append(brand_etl)
                
            rows_buffer.append(tuple(filtered_row))
            if len(rows_buffer) >= batch_size:
                execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
                total_inserted += len(rows_buffer)
                rows_buffer.clear()
        if rows_buffer:
            execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
            total_inserted += len(rows_buffer)

    dev_connection.commit()
    return total_inserted

def copy_hh_data_to_dev(dev_connection, hh_connection, source_table, target_table, brand_etl=4, where_clause=None, batch_size=5000):
    """
    Sao chép dữ liệu từ ERP HH sang DEV theo lô.
    Thêm cột brand_etl cho tất cả records.
    """
    source_cursor = stream_fetch_data_from_hh(hh_connection, source_table, where_clause=where_clause, fetch_size=batch_size)

    if source_cursor.description is None:
        raise RuntimeError(f"Không thể lấy dữ liệu từ ERP HH - cursor không hợp lệ")

    # Lấy cột từ bảng nguồn và đích
    source_columns = [desc.name for desc in source_cursor.description]
    target_columns = get_target_table_columns(dev_connection, target_table)
    
    # Lọc chỉ những cột có trong cả hai bảng
    common_columns = filter_common_columns(source_columns, target_columns)
    
    # Thêm brand_etl nếu có trong bảng đích
    if 'brand_etl' in target_columns:
        common_columns.append('brand_etl')
    
    print(f"Các cột sẽ insert từ HH: {common_columns}")

    insert_query = build_insert_statement_for_target(target_table, common_columns)

    rows_buffer = []
    total_inserted = 0

    with dev_connection.cursor() as dev_cursor:
        for row in source_cursor:
            # Chỉ lấy giá trị của các cột có trong cả hai bảng
            filtered_row = []
            for i, col_name in enumerate(source_columns):
                if col_name in common_columns:
                    filtered_row.append(row[i])
            
            # Thêm brand_etl nếu cần
            if 'brand_etl' in common_columns:
                filtered_row.append(brand_etl)
                
            rows_buffer.append(tuple(filtered_row))
            if len(rows_buffer) >= batch_size:
                execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
                total_inserted += len(rows_buffer)
                rows_buffer.clear()
        if rows_buffer:
            execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
            total_inserted += len(rows_buffer)

    dev_connection.commit()
    return total_inserted

def copy_old_data_to_dev(dev_connection, old_connection, source_table, target_table, brand_etl=0, where_clause=None, batch_size=5000):
    """
    Sao chép dữ liệu từ ERP OLD sang DEV theo lô.
    Thêm cột brand_etl cho tất cả records.
    """
    source_cursor = stream_fetch_data_from_old(old_connection, source_table, where_clause=where_clause, fetch_size=batch_size)

    if source_cursor.description is None:
        raise RuntimeError(f"Không thể lấy dữ liệu từ ERP OLD - cursor không hợp lệ")

    # Lấy cột từ bảng nguồn và đích
    source_columns = [desc.name for desc in source_cursor.description]
    target_columns = get_target_table_columns(dev_connection, target_table)
    
    # Lọc chỉ những cột có trong cả hai bảng
    common_columns = filter_common_columns(source_columns, target_columns)
    
    # Thêm brand_etl nếu có trong bảng đích
    if 'brand_etl' in target_columns:
        common_columns.append('brand_etl')
    
    print(f"Các cột sẽ insert từ OLD: {common_columns}")

    insert_query = build_insert_statement_for_target(target_table, common_columns)

    rows_buffer = []
    total_inserted = 0

    with dev_connection.cursor() as dev_cursor:
        for row in source_cursor:
            # Chỉ lấy giá trị của các cột có trong cả hai bảng
            filtered_row = []
            for i, col_name in enumerate(source_columns):
                if col_name in common_columns:
                    filtered_row.append(row[i])
            
            # Thêm brand_etl nếu cần
            if 'brand_etl' in common_columns:
                filtered_row.append(brand_etl)
                
            rows_buffer.append(tuple(filtered_row))
            if len(rows_buffer) >= batch_size:
                execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
                total_inserted += len(rows_buffer)
                rows_buffer.clear()
        if rows_buffer:
            execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
            total_inserted += len(rows_buffer)

    dev_connection.commit()
    return total_inserted

def copy_sci_data_to_dev(dev_connection, sci_connection, source_table, target_table, brand_etl=7, where_clause=None, batch_size=5000):
    """
    Sao chép dữ liệu từ ERP SCI sang DEV theo lô.
    Thêm cột brand_etl cho tất cả records.
    """
    source_cursor = stream_fetch_data_from_sci(sci_connection, source_table, where_clause=where_clause, fetch_size=batch_size)

    if source_cursor.description is None:
        raise RuntimeError(f"Không thể lấy dữ liệu từ ERP SCI - cursor không hợp lệ")

    # Lấy cột từ bảng nguồn và đích
    source_columns = [desc.name for desc in source_cursor.description]
    target_columns = get_target_table_columns(dev_connection, target_table)
    
    # Lọc chỉ những cột có trong cả hai bảng
    common_columns = filter_common_columns(source_columns, target_columns)
    
    # Thêm brand_etl nếu có trong bảng đích
    if 'brand_etl' in target_columns:
        common_columns.append('brand_etl')
    
    print(f"Các cột sẽ insert từ SCI: {common_columns}")

    insert_query = build_insert_statement_for_target(target_table, common_columns)

    rows_buffer = []
    total_inserted = 0

    with dev_connection.cursor() as dev_cursor:
        for row in source_cursor:
            # Chỉ lấy giá trị của các cột có trong cả hai bảng
            filtered_row = []
            for i, col_name in enumerate(source_columns):
                if col_name in common_columns:
                    filtered_row.append(row[i])
            
            # Thêm brand_etl nếu cần
            if 'brand_etl' in common_columns:
                filtered_row.append(brand_etl)
                
            rows_buffer.append(tuple(filtered_row))
            if len(rows_buffer) >= batch_size:
                execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
                total_inserted += len(rows_buffer)
                rows_buffer.clear()
        if rows_buffer:
            execute_values(dev_cursor, insert_query.as_string(dev_cursor), rows_buffer, page_size=batch_size)
            total_inserted += len(rows_buffer)

    dev_connection.commit()
    return total_inserted

def check_target_table_structure(dev_connection, target_table):
    """Kiểm tra cấu trúc bảng đích và partition nếu có."""
    with dev_connection.cursor() as cursor:
        # Kiểm tra xem bảng có phải là partitioned table không
        cursor.execute("""
            SELECT schemaname, tablename, tableowner 
            FROM pg_tables 
            WHERE tablename = %s
        """, (target_table,))
        
        if not cursor.fetchone():
            raise RuntimeError(f"Bảng {target_table} không tồn tại trong DEV")
        
        # Kiểm tra partition
        cursor.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE tablename LIKE %s
            ORDER BY tablename
        """, (f"{target_table}_%",))
        
        partitions = cursor.fetchall()
        if partitions:
            print(f"Bảng {target_table} là partitioned table với các partition:")
            for partition in partitions:
                print(f"  - {partition[0]}.{partition[1]}")
            return True
        else:
            print(f"Bảng {target_table} là bảng thường (không partitioned)")
            return False

def create_partition_if_needed(dev_connection, target_table, brand_etl):
    """Kiểm tra partition cho brand_etl, không tạo mới nếu đã có."""
    partition_name = f"{target_table}_brand_{brand_etl}"
    
    with dev_connection.cursor() as cursor:
        # Kiểm tra xem có partition nào handle brand_etl này chưa
        cursor.execute("""
            SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) as partition_expression
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'p' 
            AND n.nspname = 'public'
            AND c.relname LIKE %s
        """, (f"{target_table}_%",))
        
        partitions = cursor.fetchall()
        for partition in partitions:
            partition_expr = partition[1]
            # Kiểm tra xem partition này có chứa brand_etl không
            if f"({brand_etl})" in partition_expr or f"= {brand_etl}" in partition_expr:
                print(f"Partition {partition[0]} đã handle brand_etl = {brand_etl}")
                return
        
        # Nếu không tìm thấy partition phù hợp, thông báo
        print(f"Không tìm thấy partition cho brand_etl = {brand_etl}. Có thể cần tạo thủ công.")

def list_existing_partitions(dev_connection, target_table):
    """Liệt kê các partition hiện có của bảng."""
    with dev_connection.cursor() as cursor:
        cursor.execute("""
            SELECT schemaname, tablename, 
                   pg_get_expr(c.relpartbound, c.oid) as partition_expression
            FROM pg_tables pt
            JOIN pg_class c ON c.relname = pt.tablename
            WHERE pt.tablename LIKE %s
            ORDER BY pt.tablename
        """, (f"{target_table}_%",))
        
        partitions = cursor.fetchall()
        if partitions:
            print("Các partition hiện có:")
            for partition in partitions:
                print(f"  - {partition[0]}.{partition[1]}: {partition[2]}")
        return partitions

def get_target_table_columns(dev_connection, target_table):
    """Lấy danh sách cột của bảng đích."""
    with dev_connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (target_table,))
        columns = [row[0] for row in cursor.fetchall()]
        return columns

def filter_common_columns(source_columns, target_columns):
    """Lọc chỉ những cột có trong cả bảng nguồn và đích."""
    common_columns = []
    missing_columns = []
    
    for col in source_columns:
        if col in target_columns:
            common_columns.append(col)
        else:
            missing_columns.append(col)
    
    if missing_columns:
        print(f"Các cột trong bảng nguồn nhưng không có trong bảng đích: {missing_columns}")
    
    return common_columns

def build_insert_statement_for_target(target_table, column_names):
    """Sinh câu lệnh INSERT theo danh sách cột."""
    identifiers = [sql.Identifier(col) for col in column_names]
    placeholders = sql.SQL(", ").join([sql.Placeholder() for _ in column_names])
    query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
        table=sql.Identifier(target_table),
        cols=sql.SQL(", ").join(identifiers),
    )
    return query

def check_and_create_partitions(dev_connection, target_table, brand_etl_list):
    """Kiểm tra và tạo partition cho danh sách brand_etl."""
    with dev_connection.cursor() as cursor:
        # Lấy danh sách partition hiện có
        cursor.execute("""
            SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) as partition_expression
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'p' 
            AND n.nspname = 'public'
            AND c.relname LIKE %s
            ORDER BY c.relname
        """, (f"{target_table}_%",))
        
        existing_partitions = cursor.fetchall()
        print(f"Các partition hiện có cho {target_table}:")
        for partition in existing_partitions:
            print(f"  - {partition[0]}: {partition[1]}")
        
        # Kiểm tra từng brand_etl
        for brand_etl in brand_etl_list:
            found = False
            for partition in existing_partitions:
                partition_expr = partition[1]
                if f"({brand_etl})" in partition_expr or f"= {brand_etl}" in partition_expr:
                    print(f"✓ Partition cho brand_etl = {brand_etl} đã tồn tại: {partition[0]}")
                    found = True
                    break
            
            if not found:
                print(f"⚠ Không tìm thấy partition cho brand_etl = {brand_etl}")
                print(f"  Cần tạo partition thủ công hoặc sử dụng partition hiện có")

# Cập nhật hàm main()
def main():
    # Nhập tên bảng từ người dùng
    source_table = input("Nhập tên bảng nguồn (ví dụ: ir_model_data): ").strip()
    target_table = input("Nhập tên bảng đích trong DEV (ví dụ: ir_model_data_new): ").strip()
    
    if not source_table or not target_table:
        print("Tên bảng không được để trống!")
        return

    # Nhập điều kiện WHERE
    where_clause = input("Nhập điều kiện WHERE cho bảng nguồn (vd: model IN ('product.product','res.brand') AND active = true). Bỏ trống nếu không có: ").strip()
    if where_clause == "":
        where_clause = None
    
    dev_conn = None
    kn_conn = None
    pr_conn = None
    hh_conn = None
    old_conn = None
    sci_conn = None
    
    try:
        print("Đang kết nối DB DEV...")
        dev_conn = connect_db_dev()
        if dev_conn is None:
            raise RuntimeError("Không thể kết nối DB DEV")

        # Kiểm tra cấu trúc bảng đích
        is_partitioned = check_target_table_structure(dev_conn, target_table)
        
        if is_partitioned:
            print("Bảng đích là partitioned table. Kiểm tra partition...")
            check_and_create_partitions(dev_conn, target_table, [0, 1, 3, 4, 7])

        print("Đang kết nối DB ERP KN...")
        kn_conn = connect_db_erp_kn()
        if kn_conn is None:
            raise RuntimeError("Không thể kết nối DB ERP KN")

        print("Đang kết nối DB ERP PR...")
        pr_conn = connect_db_erp_pr()
        if pr_conn is None:
            raise RuntimeError("Không thể kết nối DB ERP PR")

        print("Đang kết nối DB ERP HH...")
        hh_conn = connect_db_erp_hh()
        if hh_conn is None:
            raise RuntimeError("Không thể kết nối DB ERP HH")

        print("Đang kết nối DB ERP OLD...")
        old_conn = connect_db_erp_old()
        if old_conn is None:
            raise RuntimeError("Không thể kết nối DB ERP OLD")

        print("Đang kết nối DB ERP SCI...")
        sci_conn = connect_db_erp_sci()
        if sci_conn is None:
            raise RuntimeError("Không thể kết nối DB ERP SCI")

        # Test bảng tồn tại
        for conn, name in [(kn_conn, "KN"), (pr_conn, "PR"), (hh_conn, "HH"), (old_conn, "OLD"), (sci_conn, "SCI")]:
            with conn.cursor() as cursor:
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name = %s", (source_table,))
                if not cursor.fetchone():
                    raise RuntimeError(f"Bảng {source_table} không tồn tại trong {name}")
                print(f"Bảng {source_table} tồn tại trong {name}")

        print("Kết nối thành công. Bắt đầu xóa dữ liệu...")
        
        # 1) Xóa dữ liệu bảng đích ở DEV
        truncate_target_table_dev(dev_conn, target_table)

        print(f"Bắt đầu copy dữ liệu từ {source_table} sang {target_table}...")
        
        # 2) Kéo dữ liệu từ OLD sang DEV theo lô (brand_etl = 0)
        inserted_old = copy_old_data_to_dev(dev_conn, old_conn, source_table, target_table, brand_etl=0, where_clause=where_clause, batch_size=5000)
        print(f"Đã chèn {inserted_old} dòng từ {source_table} (OLD) với brand_etl = 0")

        # 3) Kéo dữ liệu từ KN sang DEV theo lô (brand_etl = 1)
        inserted_kn = copy_kn_data_to_dev(dev_conn, kn_conn, source_table, target_table, brand_etl=1, where_clause=where_clause, batch_size=5000)
        print(f"Đã chèn {inserted_kn} dòng từ {source_table} (KN) với brand_etl = 1")

        # 4) Kéo dữ liệu từ PR sang DEV theo lô (brand_etl = 3)
        inserted_pr = copy_pr_data_to_dev(dev_conn, pr_conn, source_table, target_table, brand_etl=3, where_clause=where_clause, batch_size=5000)
        print(f"Đã chèn {inserted_pr} dòng từ {source_table} (PR) với brand_etl = 3")

        # 5) Kéo dữ liệu từ HH sang DEV theo lô (brand_etl = 4)
        inserted_hh = copy_hh_data_to_dev(dev_conn, hh_conn, source_table, target_table, brand_etl=4, where_clause=where_clause, batch_size=5000)
        print(f"Đã chèn {inserted_hh} dòng từ {source_table} (HH) với brand_etl = 4")

        # 6) Kéo dữ liệu từ SCI sang DEV theo lô (brand_etl = 7)
        inserted_sci = copy_sci_data_to_dev(dev_conn, sci_conn, source_table, target_table, brand_etl=7, where_clause=where_clause, batch_size=5000)
        print(f"Đã chèn {inserted_sci} dòng từ {source_table} (SCI) với brand_etl = 7")

        print(f"Tổng cộng đã chèn: {inserted_old + inserted_kn + inserted_pr + inserted_hh + inserted_sci} dòng")

    except Exception as exc:
        if dev_conn:
            dev_conn.rollback()
        print("Lỗi khi đồng bộ dữ liệu:", exc)
        raise
    finally:
        # Đóng tất cả connections một cách đơn giản
        connections = [kn_conn, pr_conn, hh_conn, old_conn, sci_conn, dev_conn]
        for conn in connections:
            if conn:
                try:
                    conn.close()
                except:
                    pass

if __name__ == "__main__":
    main()  

