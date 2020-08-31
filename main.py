from flask import Flask, request, jsonify, make_response
# import json
from flaskext.mysql import MySQL
from zk import ZK

mysql = MySQL()
app = Flask(__name__)

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'user'
app.config['MYSQL_DATABASE_PASSWORD'] = 'password'
app.config['MYSQL_DATABASE_DB'] = 'db_name'
app.config['MYSQL_DATABASE_HOST'] = 'host'
app.config['MYSQL_DATABASE_PORT'] = 33060
mysql.init_app(app)

@app.route('/')
def hello_world():
    return 'Hello, World!'
@app.route('/getData', methods = ['POST'])
def create_product():
    data = request.get_json()
    conn = mysql.connect()
    cursor = conn.cursor()
    message = "Không tải được dữ liệu máy chấm công. Kiểm tra lại cài đặt máy chấm công!!"
    for index in data:
        value = data.get(index)
        cond = None
        zk = ZK(value.get('ip'), int(value.get('port')), 5, int(value.get('password')), False, False)
        try:
            cond = zk.connect()
            cond.disable_device()
            # print ('Firmware Version: : {}'.format(conn.get_firmware_version()))
            # users = conn.get_users()
            attendances = cond.get_attendance()
            # print("Tong so ban ghi:" + str(len(attendances)))
            if len(attendances) > 0:
                add_items(attendances, value.get('id'), value.get('branch_hrm'), cursor, conn)
            message = "Tải dữ liệu máy chấm công thành công!!"
        except Exception as e:
            print("Process terminate : {}".format(e))
        finally:
            if cond:
                cond.enable_device()
                cond.disconnect()
    cursor.close()
    return make_response(jsonify({"data": None, "message": message}), 200)

def add_items(attendances, id, branch_hrm, cursor, conn):
    if len(attendances) > 0:
        query = "INSERT INTO `hrm_data_in_out` (`hrm_timekeeper_id`,`branch_hrm`,`uid`,`user_h_id`,`state`,`work_date`,`work_time`) VALUES "
        if len(attendances) > 5000:
            for i in list(range(0, 5000)):
                item = attendances[i]
                value = str(item).split("_")
                user_h_id = 'G' + str(value[1]).zfill(4)
                date = str(value[2]).split(" ")
                if len(date) > 1:
                    work_date = date[0]
                    work_time = date[1]
                else:
                    work_date = ""
                    work_time = ""
                uid = str(id) + "_" + str(work_date) + "_" + str(work_time) + "_" + value[0]
                state = str(value[3])
                query += "('{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(id, branch_hrm, uid, user_h_id, state, work_date, work_time)
            query = query.rstrip(',')
            query += " ON DUPLICATE KEY UPDATE hrm_timekeeper_id = VALUES(hrm_timekeeper_id),branch_hrm = VALUES(branch_hrm),user_h_id = VALUES(user_h_id),state = VALUES(state),work_date = VALUES(work_date),work_time = VALUES(work_time)"
            insert(cursor, conn, query)
            add_items(attendances[5000:], id, branch_hrm, cursor, conn)
        else:
            for i in list(range(0, len(attendances))):
                item = attendances[i]
                value = str(item).split("_")
                user_h_id = 'G' + str(value[1]).zfill(4)
                date = str(value[2]).split(" ")
                if len(date) > 1:
                    work_date = date[0]
                    work_time = date[1]
                else:
                    work_date = ""
                    work_time = ""
                uid = str(id) + "_" + str(work_date) + "_" + str(work_time) + "_" + value[0]
                state = str(value[3])
                query += "('{}', '{}', '{}', '{}', '{}', '{}', '{}'),".format(id, branch_hrm, uid, user_h_id, state, work_date,
                                                                              work_time)
            query = query.rstrip(',')
            query += " ON DUPLICATE KEY UPDATE hrm_timekeeper_id = VALUES(hrm_timekeeper_id),branch_hrm = VALUES(branch_hrm),user_h_id = VALUES(user_h_id),state = VALUES(state),work_date = VALUES(work_date),work_time = VALUES(work_time)"
            insert(cursor, conn, query)
def insert(cursor, conn,  insertCmd):
    try:
        cursor.execute(insertCmd)
        conn.commit()
        return True
    except Exception as e:
        print("Problem inserting into db: " + str(e))
        return False
if __name__ == '__main__':
     app.run()