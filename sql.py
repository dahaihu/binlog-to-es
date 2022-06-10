import time

import pymysql


def insert():
    # 连接数据库
    host = 'localhost'
    user = 'root'
    password = '669193'
    db = 'sync_es'
    conn = pymysql.connect(host=host,  # 数据库地址
                           user=user,  # 数据库用户名
                           password=password,  # 数据库密码
                           db=db,  # 数据库名称
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

    # sql语句
    sql = "insert into es_resource " \
          "(`name`, description, create_time, update_time, delete_time) " \
          "values (%s,%s,%s,%s,%s)"

    role_sql = "insert into es_resource_role(user_id, resource_id, role_id) " \
               "values (%s, %s, %s)"
    # 获取游标
    cur = conn.cursor()
    for i in range(0, 1000):
        # 参数化方式传参
        now = int(time.time())
        if i % 1 == 0:
            conn.commit()
            time.sleep(1)
        insert_result = cur.execute(sql,
                                    ['name_{}'.format(i),
                                     'description_{}'.format(i),
                                     now,
                                     now,
                                     now])
        cur.execute(role_sql, [cur.lastrowid, cur.lastrowid, cur.lastrowid])

        print("insert {}".format(i))
    conn.commit()
    cur.close()
    # 关闭连接
    conn.close()


"""
insert into es_resource_role(user_id, resource_id, role_id, create_time, 
update_time, delete_time) values(3, 9, 3, 1000, 1000, 1000);

insert into es_resource(name, description, create_time, update_time, 
delete_time) values('name', 'description1', 100, 100, 100);
"""

if __name__ == '__main__':
    start_time = time.time()
    insert()
    print("cost time is {}".format(time.time() - start_time))
