from flask import Flask, request, render_template, url_for, jsonify
import mysql.connector
import datetime
import pandas as pd
import numpy as np
import plotly.express as px
from lifetimes import GammaGammaFitter
import plotly
from lifetimes import BetaGeoFitter

app = Flask(__name__)

def get_connection():
    conn = mysql.connector.connect(
        host = "us-cdbr-east-05.cleardb.net", 
        user = "bb7a3d0b1f6dbf", 
        password = "e159ae8e", 
        database = "heroku_f803958b7a21a29"
    )
    return conn


################ 作業管理 ################
# 每日訂單進度/order_progress
@app.route("/")
@app.route("/order_progress")
def order_progress():
    return render_template("order-progress.html", orders = get_order_progress())


# 生產機器排程/machine
@app.route("/machine")
def machine():
    today = datetime.datetime.today().day
    schedules = process_machine_schedule()
    return render_template("machine.html", schedules = schedules, today = today)
    # return render_template("machine.html")

# 庫存管理/material
@app.route("/material")
def material():
    return render_template("material.html")

# 新增訂單/scheduling
@app.route("/planning", methods = ['POST', 'GET'])
def planning():
    if request.method == 'GET':
        return render_template("planning.html", result = '')
    if request.method == 'POST':
        c_id = request.form.get('c_id')
        p_id = request.form.get('p_id')
        p_num = request.form.get('p_num')
        if c_id == '':
            return render_template("planning.html", result = ['請輸入顧客編號'], c_id = c_id, p_id = p_id, p_num = p_num)
        if p_id == '':
            return render_template("planning.html", result = ['請輸入產品編號'], c_id = c_id, p_id = p_id, p_num = p_num)
        if p_num == '':
            return render_template("planning.html", result = ['請輸入顧產品數量'], c_id = c_id, p_id = p_id, p_num = p_num)
        if int(c_id) not in range(1, 31):
            return render_template("planning.html", result = ['查無顧客'], c_id = c_id, p_id = p_id, p_num = p_num)
        p_list = get_product_list()
        if int(p_id) not in p_list:
            return render_template("planning.html", result = ['查無產品'], c_id = c_id, p_id = p_id, p_num = p_num)
        o_id, o_price, start, end = add_new_order(c_id, p_id, p_num)
        result = [f"訂單編號： {o_id}", f"訂單價格： {o_price}", f"開始日期： {start}", f"結束日期： {end}"]
        return render_template("planning.html", result = result, c_id = c_id, p_id = p_id, p_num = p_num)

# 庫存管理/material
@app.route("/material/data")
def material_data():
    data = {"quantity": [18000, 20000, 10000, 5000, 15000, 10000, 20000, 15000, 25000, 20000, 30000, 25000, 20000]}
    return jsonify(data)

def get_order_progress():
    date = "2021/07/15"
    today = datetime.datetime.today()
    print(today)
    # query
    sql = f"""
    SELECT `order`.O_ID, schedule.status 
    FROM heroku_f803958b7a21a29.order
    LEFT JOIN heroku_f803958b7a21a29.schedule ON `order`.O_ID = schedule.O_ID
    WHERE `order`.Order_date <= '{today}'
    AND `order`.Delivery_date >= '{today}';
    """
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchall()
    table = []
    for order in query:
        order_id = order[0]
        status = order[1]
        percentage = ''
        if status == 1:
            percentage = '20%'
        elif status == 2:
            percentage = '40%'
        elif status == 3:
            percentage = '60%'
        elif status == 4:
            percentage = '80%'
        elif status == 5:
            percentage = 'Complete!'
        table.append([order_id, percentage, status])
    return table

def get_machine_schedule():
    # get month time
    month_start_time = "2022/01/01"
    month_end_time = "2022/01/31"
    # query
    sql = f"""
    SELECT `machine-schedule`.M_ID, schedule.Start_date, schedule.End_date 
    FROM `machine-schedule` LEFT JOIN schedule ON `machine-schedule`.Sch_ID = schedule.Sch_ID 
    WHERE Start_date BETWEEN '{month_start_time} 00:00:00' AND '{month_end_time} 23:59:59' 
    OR End_date BETWEEN '{month_start_time} 00:00:00' AND '{month_end_time} 23:59:59';
    """
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchall()
    return query
    
def process_machine_schedule():
    today = datetime.datetime.today()
    # empty schedule table
    table = []
    for i in range(30):
        table.append([])
    # put schedule in table
    query = get_machine_schedule()
    for schedule in query:
        m_id = schedule[0]
        start_day = schedule[1]
        end_day = schedule[2]
        if m_id == 2:
            print(start_day, end_day)
        if(start_day.month != today.month):
            start_day = datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")
        if(end_day.month != today.month):
            end_day = datetime.datetime.strptime("2021-01-31", "%Y-%m-%d")
        for day in range(start_day.day, end_day.day + 1):
            table[m_id-1].append(day)
    return table

def get_product_list():
    # query
    sql = "SELECT P_ID FROM heroku_f803958b7a21a29.product;"
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchall()
    p_list = []
    for product in query:
        p_list.append(product[0])
    return p_list

def add_new_order(c_id, p_id, p_num):
    p_num = int(p_num)
    days = p_num//1000
    o_id = get_last_order()[0] + 1
    o_price = get_product_price(p_id) * p_num
    table = last_available_time()
    bar, bar_i = min((day[1], day[0]) for day in table[0:6])
    cnc, cnc_i = min((day[1], day[0]) for day in table[6:18])
    surface, surface_i = min((day[1], day[0]) for day in table[18:24])
    packing, packing_i = min((day[1], day[0]) for day in table[24:30])
    start_date = max([bar, cnc, surface, packing])
    start = datetime.datetime.strptime(f"2022-01-{start_date}", "%Y-%m-%d")
    end = datetime.datetime.strptime(f"2022-01-{start_date+days}", "%Y-%m-%d")
    if start_date > 31:
        start = '2022-02-01'
        end = '2022-02-01'
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO heroku_f803958b7a21a29.order VALUES ('{o_id}', '{o_price}', '{start}', '{end}', '{c_id}', '{o_id}');")
    cur.execute(f"INSERT INTO heroku_f803958b7a21a29.schedule VALUES ('{o_id}', '{start}', '{end}', '1', '{o_id}');")
    cur.execute(f"INSERT INTO heroku_f803958b7a21a29.`machine-schedule` VALUES ('{bar_i+1}', '{o_id}');")
    cur.execute(f"INSERT INTO heroku_f803958b7a21a29.`machine-schedule` VALUES ('{cnc_i+1}', '{o_id}');")
    cur.execute(f"INSERT INTO heroku_f803958b7a21a29.`machine-schedule` VALUES ('{surface_i+1}', '{o_id}');")
    cur.execute(f"INSERT INTO heroku_f803958b7a21a29.`machine-schedule` VALUES ('{packing_i+1}', '{o_id}');")
    conn.commit()
    return o_id, o_price, start, end

def get_last_order():
    # query
    sql = "SELECT * FROM heroku_f803958b7a21a29.order ORDER BY O_ID DESC"
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchone()
    print(f"last order: {query}")
    return query

def last_available_time():
    today = datetime.datetime.today()
    # empty schedule table
    table = []
    for i in range(30):
        table.append([i, today.day + 1])
    # put schedule in table
    query = get_machine_schedule()
    for schedule in query:
        m_id = schedule[0]
        end_day = schedule[2]
        if(end_day.month != today.month):
            end_day = datetime.datetime.strptime("2021-01-31", "%Y-%m-%d")
        if table[m_id-1][1] < end_day.day:
            table[m_id-1][1] = end_day.day
    return table

def get_product_price(p_id):
    # query
    sql = f"SELECT P_price FROM heroku_f803958b7a21a29.product WHERE P_ID = {p_id};"
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchone()[0]
    return query

################ 顧客關係管理 ################
# 年銷售額/annual_sales
@app.route("/annual_sales")
def annual_sales():
    return render_template("annual-sales.html")

# 季銷售額/quarterly_sales
@app.route("/quarterly_sales")
def quarterly_sales():
    return render_template("quarterly-sales.html")

# 月銷售額/monthly_sales
@app.route("/monthly_sales")
def monthly_sales():
    return render_template("monthly-sales.html")

# 顧客終身價值/clv
@app.route("/clv")
def clv():
    # return render_template("clv.html", tables=[getCLV().to_html(classes='data')], titles=getCLV().columns.values)
    return render_template("clv.html", clvs = getCLV())

# 顧客分群/kmeans
@app.route("/kmeans")
def kmeans():
    return render_template("kmeans.html")

# 顧客資料查詢/customer_search
@app.route("/customer_search")
def customer_search():
    return render_template("customer-search.html", customers = customer_table())

# 訂單資料查詢/order-search
@app.route("/order-search")
def order_search():
    return render_template("order-search.html", orders = order_table())

def customer_table():
    table = []
    sql = f"""
    SELECT customer.C_Name, customer.City, customer.Phone, customer.`E-mail`, customer.Recency, customer.CLV 
    FROM heroku_f803958b7a21a29.customer;
    """
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchall()
    for customer in query:
        name = customer[0]
        city = customer[1]
        phone = customer[2]
        email = customer[3]
        recency = customer[4]
        clv = customer[5]
        table.append([name, city, phone, email, recency, clv])
    return table

def order_table():
    table = []
    sql = f"""
    SELECT `order`.O_ID, customer.C_Name, `order`.Order_date, `order`.Delivery_date, `order`.O_price, schedule.status
    FROM heroku_f803958b7a21a29.order, heroku_f803958b7a21a29.customer, heroku_f803958b7a21a29.schedule 
    WHERE `order`.C_ID = customer.C_ID AND `order`.O_ID = schedule.O_ID;
    """
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchall()
    for customer in query:
        order_id = customer[0]
        customer_name = customer[1]
        order_date = customer[2]
        delivery_date = customer[3]
        price = customer[4]
        status = customer[5]
        table.append([order_id, customer_name, order_date, delivery_date, price, status * 20])
    return table

def getRFM():
    # read RFM-table.csv
    df = pd.read_csv("RFM-table.csv")
    # convert Timestamp type to Datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # RFM指標
    df['days'] = (datetime.datetime.strptime('2022-01-06', '%Y-%m-%d') - df['Date']).dt.days
    rfm = df.groupby(by=['C_ID']).agg(
        recency=('days', min),
        frequency=('C_ID', 'size'),
        monetary=('Sales_Amount', 'mean'),
        senior=('days', max),
        since=('Date', min)
    )
    rfm['log_monetary'] = np.log(rfm['monetary'])
    #print(rfm)
    return rfm

# 計算Customer lifetime value
def getCLV():
    rfm = getRFM()
    retain = rfm[rfm['frequency']>0] # 篩出購買一次以上的回購顧客
    ggf = GammaGammaFitter(penalizer_coef=0.001)
    ggf.fit(retain['frequency'], retain['monetary'])
    ggf.conditional_expected_average_profit(rfm['frequency'], rfm['monetary'])
    
    bgf = BetaGeoFitter(penalizer_coef = 0.001).fit(rfm['frequency'], rfm['recency'], rfm['senior'])

    clv = ggf.customer_lifetime_value(
        bgf,
        rfm['frequency'],
        rfm['recency'],
        rfm['senior'],
        rfm['monetary'],
        time=12, # 月份
        discount_rate=0.01 # 每月調整後的折現率(默認0.01)
    )
    clv = clv.tolist()
    table = []
    for i in range(len(clv)):
        table.append([i+1, round(clv[i], 2)])
    # clv = pd.DataFrame(clv).sort_values(by=['clv'], ascending=False, inplace=False)
    # # print(clv.clv.tolist())
    # # print(clv.index.tolist())
    # clv_values = clv.clv.tolist()
    # index = clv.index.tolist()
    # table = []
    # for i in range(len(index)):
    #     table.append([index[i], round(clv_values[i], 2)])
    # print(table)
    return table

def bubble():
    rfm = getRFM()
    # 顧客分群
    # 為避免分群產生誤差，進行單位的標準化
    from sklearn import preprocessing
    scaled_data = preprocessing.scale(rfm.loc[:,['recency','frequency','monetary']])
    rfm_scale = pd.DataFrame(scaled_data, columns = ['recency','frequency','monetary'])

    # 利用Kmeans分群
    from sklearn.cluster import KMeans

    model = KMeans(n_clusters=10, random_state=2021).fit(rfm_scale)
    rfm['Labels'] = model.labels_ # 將分群結果加入變數
    rfm['Labels'].value_counts().sort_index()

    # 依據分群結果計算每群的RFM指標
    rfm_clust = rfm.groupby(by=['Labels']).agg(
        # 每群平均recency
        recency=('recency', 'mean'),
        # 每群平均frequency
        frequency=('frequency', 'mean'),
        # 每群平均monetary
        monetary=('monetary', 'mean'),
        # 每群size
        size=('Labels', 'size')
    )
    rfm_clust['revenue'] = rfm_clust['size']*rfm_clust['monetary']/1000
    #print(rfm_clust)

    # 泡泡圖
    rfm_clust['cluster'] = rfm_clust.index
    fig = px.scatter(rfm_clust, x="frequency", y="monetary", size="revenue", color="recency",
                    hover_name="cluster", log_x=True, log_y=True, size_max=110,
                    text="size", title="Customer Segements",
                    labels={ "frequency": "Frequency (log)", "monetary": "Average Transaction Amount (log)", "recency": "Recency"})
    #使圖片的html檔能被編輯
    plotly.offline.plot(fig,filename='bubble.html',config={'displayModeBar': False})
    return fig.show()


################ 訂單查詢系統 ################
@app.route("/customer-order/<id>")
@app.route("/customer-order/<id>/<order_id>")
def customer_order(id, order_id = None):
    print(id)
    orders = get_customer_order(id)
    if(order_id):
        selected_order = get_selected_order(id, order_id)
    else:
        selected_order = orders[-1]    
    return render_template("customer-order.html", orders = orders, selected_order = selected_order)

# @app.route("/customer-order/<id>/<order_id>")
# def customer_selected_order(id, order_id):
#     print(id)
#     orders = get_customer_order(id)
#     selected_order = get_selected_order(id, order_id)
#     return render_template("customer-order.html", orders = orders, selected_order = selected_order)

def get_customer_order(id):
    table = []
    sql = f"""
    SELECT `order`.O_ID, `order`.O_price, `order`.Order_date, `order`.Delivery_date, `schedule`.status
    FROM heroku_f803958b7a21a29.order, heroku_f803958b7a21a29.schedule
    WHERE `order`.C_ID = {id} AND `order`.Sch_ID = `schedule`.Sch_ID;
    """
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchall()
    for order in query:
        order_id = order[0]
        price = order[1]
        order_date = order[2]
        delivery_date = order[3]
        status = order[4]
        table.append([order_id, price, order_date, delivery_date, status])
    return table

def get_selected_order(id, order_id):
    sql = f"""
    SELECT `order`.O_ID, `order`.O_price, `order`.Order_date, `order`.Delivery_date, `schedule`.status
    FROM heroku_f803958b7a21a29.order, heroku_f803958b7a21a29.schedule
    WHERE `order`.C_ID = {id} AND `order`.Sch_ID = `schedule`.Sch_ID AND `order`.O_ID = {order_id};
    """
    # execute sql query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    query = cur.fetchone()
    print(query)
    order_id = query[0]
    price = query[1]
    order_date = query[2]
    delivery_date = query[3]
    status = query[4]
    return [order_id, price, order_date, delivery_date, status]


################ 啟動伺服器 ################
if __name__ == "__main__":
    app.run(debug = True)