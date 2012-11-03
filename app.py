# coding: utf-8
# sudo aptitude install -y python-flask python-mysqldb python-routes
from __future__ import with_statement

import time
import jinja2

try:
    import MySQLdb
    from MySQLdb.cursors import DictCursor
except ImportError:
    import pymysql as MySQLdb
    from pymysql.cursors import DictCursor 

from flask import Flask, request, g, redirect, \
             render_template, _app_ctx_stack, Response

import json, os
from collections import defaultdict

config = {}


RECENT_SOLD_KEY = "<!--# include recent_sold -->"

app = Flask(__name__, static_url_path='')

def load_config():
    global config
    print "Loading configuration"
    env = os.environ.get('ISUCON_ENV') or 'local'
    with open('../config/common.' + env + '.json') as fp:
        config = json.load(fp)

def connect_db():
    global config
    host = config['database']['host']
    port = config['database']['port']
    username = config['database']['username']
    password = config['database']['password']
    dbname   = config['database']['dbname']
    db = MySQLdb.connect(host=host, port=port, db=dbname, user=username, passwd=password, cursorclass=DictCursor, charset="utf8")
    return db

def init_db():
    print "Initializing database"
    with connect_db() as cur:
        with open('../config/database/initial_data.sql') as fp:
            for line in fp.readlines():
                line = line.strip()
                if len(line) > 0:
                    cur.execute(line)
    db = connect_db()
    initialize()

def get_recent_sold(db):
    cur = db.cursor()
    cur.execute('''SELECT stock.seat_id, variation.name AS v_name, ticket.name AS t_name, artist.name AS a_name FROM stock
        JOIN variation ON stock.variation_id = variation.id
        JOIN ticket ON variation.ticket_id = ticket.id
        JOIN artist ON ticket.artist_id = artist.id
        WHERE order_id IS NOT NULL
        ORDER BY order_id DESC LIMIT 10''')
    recent_sold = cur.fetchall()
    cur.close()
    return recent_sold


_recent_sold_t = None
_recent_sold = b''

def render_recent_sold(db):
    global _recent_sold, _recent_sold_t
    if _recent_sold_t is None:
        _recent_sold_t = jinja2.Template(open('templates/recent_sold.html').read().decode('utf-8'))
    _recent_sold = _recent_sold_t.render(recent_sold=get_recent_sold(db))

ARTISTS = None
ARTIST_TICKETS = defaultdict(list)
TICKETS = {}
VARIATIONS = {}

def initialize():
    print "initialize()"
    while True:
        # db が起動するのを待つ.
        try:
            db = connect_db()
            break
        except Exception as e:
            print e
            time.sleep(1)
            continue

    render_recent_sold(db)

    cur = db.cursor()
    cur.execute('SELECT * FROM artist')
    global ARTISTS
    ARTISTS = cur.fetchall()

    global ARTIST_TICKETS, TICKETS, VARIATIONS
    ARTIST_TICKETS.clear()
    TICKETS.clear()
    VARIATIONS.clear()

    cur.execute('SELECT * FROM ticket')
    tickets = cur.fetchall()
    for ticket in tickets:
        ARTIST_TICKETS[ticket['artist_id']].append(ticket)
        TICKETS[ticket['id']] = ticket

        cur.execute(
            '''SELECT COUNT(*) AS cnt FROM variation
                INNER JOIN stock ON stock.variation_id = variation.id
                WHERE variation.ticket_id = %s AND stock.order_id IS NULL''',
            ticket['id']
        )
        ticket['count'] = cur.fetchone()['cnt']

        cur.execute(
            'SELECT id, name FROM variation WHERE ticket_id = %s',
            ticket['id']
        )
        variations = cur.fetchall()
        ticket['variations'] = variations

        for variation in variations:
            variation['ticket'] = ticket
            VARIATIONS[variation['id']] = variation
            cur.execute(
                'SELECT seat_id, order_id FROM stock WHERE variation_id = %s',
                variation['id']
            )
            stocks = cur.fetchall()
            variation['stock'] = {}
            remain = 0
            for row in stocks:
                variation['stock'][row['seat_id']] = row['order_id']
                if row['order_id'] is None:
                    remain += 1
            variation['vacancy'] = remain

    print "initialize() end"


def get_db():
    top = _app_ctx_stack.top
    if not hasattr(top, 'db'):
        top.db = connect_db()
    return top.db


@app.teardown_appcontext
def close_db_connection(exception):
    top = _app_ctx_stack.top
    if hasattr(top, 'db'):
        top.db.close()

@app.route("/")
def top_page():
    cur = get_db().cursor()
    return render_template('index.html',
            artists=ARTISTS).replace(RECENT_SOLD_KEY, _recent_sold)

@app.route("/artist/<int:artist_id>")
def artist_page(artist_id):
    for artist in ARTISTS:
        if artist['id'] == artist_id:
            break
    tickets = ARTIST_TICKETS[artist_id]
    return render_template(
        'artist.html',
        artist=artist,
        tickets=tickets,
    ).replace(RECENT_SOLD_KEY, _recent_sold)

@app.route("/ticket/<int:ticket_id>")
def ticket_page(ticket_id):
    cur = get_db().cursor()
    
    ticket = TICKETS[ticket_id]
    variations = ticket['variations']

    return render_template(
        'ticket.html',
        ticket=ticket,
        variations=variations,
    ).replace(RECENT_SOLD_KEY, _recent_sold)

@app.route("/buy", methods=['POST'])
def buy_page():
    variation_id = request.values['variation_id']
    member_id = request.values['member_id']

    db = get_db()
    cur = db.cursor()
    cur.execute('BEGIN')
    cur.execute(
        'INSERT INTO order_request (member_id) VALUES (%s)',
        (member_id)
    )
    order_id = db.insert_id()
    rows = cur.execute(
        'UPDATE stock SET order_id = %s WHERE variation_id = %s AND order_id IS NULL ORDER BY RAND() LIMIT 1',
        (order_id, variation_id)
    )
    if rows > 0:
        cur.execute(
            'SELECT seat_id FROM stock WHERE order_id = %s LIMIT 1',
            (order_id)
        );
        stock = cur.fetchone()
        cur.execute('COMMIT')
        render_recent_sold(db)
        variation = VARIATIONS[int(variation_id)]
        variation['vacancy'] -= 1
        variation['stock'][stock['seat_id']] = member_id
        variation['ticket']['count'] -= 1
        return render_template('complete.html', seat_id=stock['seat_id'], member_id=member_id)
    else:
        cur.execute('ROLLBACK')
        return render_template('soldout.html')

@app.route("/admin", methods=['GET', 'POST'])
def admin_page():
    if request.method == 'POST':
        init_db()
        return redirect("/admin")
    else:
        return render_template('admin.html')

@app.route("/admin/order.csv")
def admin_csv():
    cur = get_db().cursor()
    cur.execute('''SELECT order_request.*, stock.seat_id, stock.variation_id, stock.updated_at
         FROM order_request JOIN stock ON order_request.id = stock.order_id
         ORDER BY order_request.id ASC''')
    orders = cur.fetchall()
    cur.close()

    body = ''
    for order in orders:
        body += ','.join([str(order['id']), order['member_id'], order['seat_id'], str(order['variation_id']), order['updated_at'].strftime('%Y-%m-%d %X')])
        body += "\n"
    return Response(body, content_type="text/csv")

load_config()
initialize()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", '5000'))
    app.run(debug=1, host='0.0.0.0', port=port)
