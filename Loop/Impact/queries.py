import MySQLdb

mysql_cn = MySQLdb.connect(host='localhost', port=3306, user='root',
                           passwd='root',
                           db='digitalgreen',
                           charset='utf8',
                           use_unicode=True)

def onrun_query(query):
    cursor = mysql_cn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result


