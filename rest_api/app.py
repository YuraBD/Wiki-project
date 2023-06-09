from flask import Flask, request, jsonify
from flask_cassandra import CassandraCluster
import datetime
import time

app = Flask(__name__)
app.config['CASSANDRA_NODES'] = ['localhost']
cassandra = CassandraCluster()

##### SET A #####
@app.route('/domain_page_create_stats', methods=['GET'])
def get_domain_page_create_stats():
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    end_time = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    end_time = end_time.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S+0000')
    start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    start_time = start_time.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S+0000')
    result = session.execute(f"""
        SELECT *
        FROM domain_page_create_stats
        WHERE time_start >= '{start_time}' AND time_end <= '{end_time}' ALLOW FILTERING
    """)

    statistics = {}
    for row in result:
        time_key = f'{row.time_start},{row.time_end}'
        if time_key not in statistics:
            statistics[time_key] = {}
        statistics[time_key][row.domain] = row.page_count

    for st_key in statistics:
        st_value = []
        for key, value in statistics[st_key].items():
            st_value.append({key:value})
        statistics[st_key] = st_value

    return jsonify([
        {'time_start': key.split(',')[0], 'time_end': key.split(',')[1], 'statistics': value} 
        for key, value in statistics.items()
    ])

@app.route('/bot_page_create_stats', methods=['GET'])
def get_bot_page_create_stats():
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    end_time = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    end_time = end_time.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S+0000')
    start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    start_time = start_time.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S+0000')
    result = session.execute(f"""
        SELECT *
        FROM bot_page_create_stats
        WHERE time_start >= '{start_time}' AND time_end <= '{end_time}' ALLOW FILTERING
    """)
    
    statistics = {}
    for row in result:
        if row.domain not in statistics:
            statistics[row.domain] = 0
        statistics[row.domain] += row.bot_page_count

    st_value = []
    for domain, count in statistics.items():
        st_value.append({"domain": domain, "created_by_bots": count})

    return jsonify({'time_start': start_time, 'time_end': end_time, 'statistics': st_value})

@app.route('/user_page_create_stats', methods=['GET'])
def get_user_page_create_stats():
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    end_time = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    end_time = end_time.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S+0000')
    start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    start_time = start_time.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S+0000')
    result = session.execute(f"""
        SELECT *
        FROM user_page_create_stats
        WHERE time_start >= '{start_time}' AND time_end <= '{end_time}' ALLOW FILTERING
    """)
    result = [{'user_id': row.user_id, 'user_name': row.user_name, 'time_start': row.time_start, 'time_end': row.time_end, 'page_titles': row.page_titles, 'page_count': row.page_count} for row in result]

    return jsonify(result[:20])

##### SET B #####
@app.route('/domains', methods=['GET'])
def get_domains():
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    result = session.execute("""
        SELECT domain
        FROM domain_page_create_stats ALLOW FILTERING
    """)
    result = list(set([row.domain for row in result]))
    return jsonify(result)

@app.route('/user/pages', methods=['GET'])
def get_pages_by_user():
    user_id = int(request.args.get('user_id'))
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    result = session.execute(f"""
        SELECT page_titles, user_id
        FROM user_page_create_stats ALLOW FILTERING
    """)
    filtered_result = []
    for row in result:
        if row.user_id == user_id:
            filtered_result += row.page_titles
    return jsonify(filtered_result)

@app.route('/domain/articles', methods=['GET'])
def get_article_count():
    domain = request.args.get('domain')
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    result = session.execute(f"""
        SELECT SUM(page_count)
        FROM domain_page_create_stats
        WHERE domain = '{domain}' ALLOW FILTERING
    """)
    return jsonify({'article_count': result[0][0]})

@app.route('/pages', methods=['GET'])
def get_page_by_id():
    page_id = request.args.get('page_id')
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    result = session.execute(f"""
        SELECT page_id, page_title, user_id
        FROM raw_data_table
        WHERE page_id = {page_id} ALLOW FILTERING
    """)
    return jsonify({"page_id": result[0].page_id, "page_title": result[0].page_title, "user_id": result[0].user_id } if result else None)

@app.route('/users/pages', methods=['GET'])
def get_users_by_time_range():
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    session = cassandra.connect()
    session.set_keyspace('wikimedia')
    result = session.execute(f"""
        SELECT user_id, user_name, page_count
        FROM user_page_create_stats
        WHERE time_start >= '{start_time}' AND time_end <= '{end_time}' ALLOW FILTERING
    """)

    result_updated = {}
    for row in result:
        if row.user_id not in result_updated:
            result_updated[row.user_id] = [row.user_name, 0]
        result_updated[row.user_id][1] += row.page_count

    return jsonify([{'user_id': user_id, 'user_name': user_name, 'page_count': page_count} for user_id, [user_name, page_count] in result_updated.items()])

if __name__ == '__main__':
    app.run(debug=True)
