from flask import Flask, jsonify, request
import psycopg2 

app = Flask(__name__)

# sshtunnel.SSH_TIMEOUT = 5.0
# sshtunnel.TUNNEL_TIMEOUT = 5.0

postgres_hostname = "rajje.db.elephantsql.com"  # You will have your own here
postgres_host_port = 5432

def db_connect():
    # with SSHTunnelForwarder(
    #     ('ssh.pythonanywhere.com'),
    #     ssh_username='bramantyo',
    #     ssh_password='Adminfbo1o282!',
    #     remote_bind_address=(postgres_hostname, postgres_host_port)
    # ) as tunnel:
    #     connection = psycopg2.connect(
    #         user='wwkdipcf', password='5eiolHpr5I_PileaA1HoyRei7qOgzJyr',
    #         host='127.0.0.1', port=tunnel.local_bind_port,
    #         database='wwkdipcf',
    #     )

    conn = psycopg2.connect(
            host='rajje.db.elephantsql.com', 
            database='wwkdipcf', 
            user='wwkdipcf', password='5eiolHpr5I_PileaA1HoyRei7qOgzJyr',
            port=5432
        )
    # conn = psycopg2.connect(
    #         host='103.150.197.96', 
    #         database='postgres', 
    #         user='postgres', password='dsbatch13!',
    #         port=5431
    #     )
    cur = connection.cursor()
    return connection, cur

@app.route("/")
def index():
    return "Test API"

@app.route("/health_check")
def health_check():
    db_connect()
    return ""

@app.route("/user", methods=['GET', 'POST'])
def user():
    conn, cur = db_connect()

    if request.method == 'GET':
        query = '''
            SELECT * FROM public.users;
        '''

        cur.execute(query)
        data_user = cur.fetchall()

        result = []

        for row in data_user:
            row_dict = {}

            for i, col in enumerate(cur.description):
                row_dict[col.name] = row[i]

            result.append(row_dict)

        return jsonify(result)
    elif request.method == 'POST':
        name = request.form.get("name")
        city = request.form.get("city")
        telp = request.form.get("telp")

        query = f'''
            INSERT INTO public.users(name,city,telp) values ('{name}', '{city}', '{telp}');
        '''

        cur.execute(query)
        conn.commit()

        return jsonify({"name": name, "city": city, "telp":telp})
    else:
        pass

    

if __name__ == "__main":
    app.run(debug=True, host="0.0.0.0")
