import psycopg2 as pg
import psycopg2.extras
import psycopg2.pool
import urllib.parse as parse
import os, string, random
from datetime import datetime
from collections import Counter
import numpy as np
from functools import wraps 

pg_url = parse.urlparse(os.environ["HAGG_DB"])
pool = pg.pool.ThreadedConnectionPool(10, 200,
    database = pg_url.path[1:],
    user = pg_url.username,
    password = pg_url.password,
    host = pg_url.hostname
)

def db(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        with pool.getconn() as conn:
            with conn.cursor(cursor_factory=pg.extras.DictCursor) as cursor:
                ret = f(cursor, *args, **kwargs)
        pool.putconn(conn)
        return ret
    return decorator

def normalize_word(word):
    return word.lower().translate(str.maketrans("", "", string.punctuation+'’”‘“'))

@db
def upsert_docs(cursor, docs):
    if len(docs) == 0: return
    tuples = []
    for doc in docs:
        tuples.append((doc['id'], doc['title'], 
            doc.get('url', "https://news.ycombinator.com/item?id="+str(doc['id'])), 
            datetime.fromtimestamp(doc['time']), doc['by'], doc.get('descendants', 0), 
            doc['score']))

    s = ",".join(["%s"]*len(docs))
    cursor.execute("""insert into docs
            (id, title, url, time, hn_user, comments, score) values {0}
            on conflict (id) do update set score = excluded.score,
            comments = excluded.comments""".format(s), tuples)

@db
def new_doc_ids(cursor, all_ids):
    cursor.execute("select a.id " +
            "from (select id from unnest(%s) as id) a " +
            "left join docs on docs.id = a.id " +
            "where docs.id is null", (all_ids, ))
    print(cursor.query)
    ids = [row['id'] for row in cursor.fetchall()]
    return ids

@db
def get_words(cursor, words):
    cursor.execute("""select wv.word, wv.vector, wc.count
            from word_vectors wv
            join word_counts wc on wc.word = wv.word
            where wv.word = any(%s)""", (words, ))
    return cursor.fetchall()

@db
def count_words_from_titles(cursor, titles):
    if len(titles) == 0: return
    words = Counter([normalize_word(word) for title in titles for word in title.split(" ")])
    s = ",".join(["%s"]*len(words))
    cursor.execute("""insert into word_counts (word, count) values {0} on conflict 
            (word) do update set count = word_counts.count + excluded.count""".format(s),
            list(words.items()))
    print(cursor.query)

@db
def upsert_word_vectors(cursor, rows):
    if len(rows) == 0: return
    s = ','.join(['%s'] * len(rows))
    cursor.execute("""insert into word_vectors (word, vector) values {0}
            on conflict do nothing""".format(s), rows)

@db
def most_recent(cursor):
    cursor.execute("select id from docs order by time desc limit 500")
    print(cursor.query)
    ids = [ row['id'] for row in cursor.fetchall() ]
    return docs_and_vectors(ids)

@db
def docs_by_id(cursor, ids):
    cursor.execute("""select id, title, url, time, comments, hn_user, score
            from docs where id = any(%s)""", (ids, ))
    print(cursor.query)
    return cursor.fetchall()

def docs_and_vectors(ids):
    docs = docs_by_id(ids)
    titles = []
    for doc in docs:
        titles.append([normalize_word(word) for word in doc['title'].split(" ")])
    vectors = title_vectors(titles)
    return docs, vectors

@db
def title_vectors(cursor, titles):
    words = list({word for title in titles for word in title})
    cursor.execute("""select wv.word, wv.vector, wc.count / (
                select sum(count) from word_counts
            )::float as frequency
            from word_vectors wv
            join word_counts wc on wc.word = wv.word
            where wv.word = any(%s)""", (words, ))
    word_vectors = { row['word']: (np.array(row['vector']), row['frequency']) 
            for row in cursor.fetchall() if row['vector']}

    vector_matrix = []
    for title in titles:
        title_vector = np.zeros(shape=(300,), dtype=float)
        for word, count in list(Counter(title).items()):
            if word in word_vectors:
                vector, frequency = word_vectors[word]
                title_vector += vector * (count * frequency)
        vector_matrix.append(title_vector)
    return np.array(vector_matrix)

def doc_by_id(id):
    docs = docs_by_id([id])
    return docs[0]

@db
def vector_from_token(cursor, token):
    v = np.zeros(shape=(300,), dtype=float)
    seen = set()
    if token is None:
        token = create_cookie()
        return token, v, seen

    cursor.execute("""select d.id, d.title, d.url, d.time, d.comments, d.hn_user, d.score
            from clicks cl
            join cookies c on c.id = cl.cookie_id
            join docs d on d.id = cl.doc_id
            where token = %s""", (token, ))
    docs = cursor.fetchall()
    if len(docs) == 0:
        return token, v, seen
    words = [normalize_word(word) for doc in docs 
        for word in doc['title'].split(" ")]
    v = title_vectors([words]) / len(docs)
    seen = set([doc['id'] for doc in docs])
    return token, v, seen

@db
def create_cookie(cursor):
    token = ''.join(random.choice(string.ascii_lowercase) for i in range(32))
    cursor.execute("insert into cookies (token) values (%s)", (token, ))
    return token

@db
def click(cursor, doc_id, token):
    cursor.execute("""insert into clicks (cookie_id, doc_id)
            select id as cookie_id, %s as doc_id
            from cookies
            where token = %s""", (doc_id, token))
    print(cursor.query)
    return doc_by_id(doc_id)
