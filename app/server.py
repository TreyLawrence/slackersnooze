from flask import Flask, redirect, request, render_template
from multiprocessing import Process, Queue
from poll import poll
from datetime import datetime, timedelta
from functools import wraps
import tldextract
import db
import numpy as np
import numpy.linalg as la
from scipy.spatial.distance import mahalanobis

app = Flask(__name__)
q = Queue()
d = ([], np.zeros(shape=(300,)))

def q_get():
    global d
    if not q.empty():
        d = q.get()
    return d

def docs(): return q_get()[0]
def vectors(): return q_get()[1]

def with_vector(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        start = datetime.now()
        token, clicked, seen = db.vector_from_token(request.cookies.get("token"))
        response = f(*args, clicked=clicked, seen=seen, **kwargs)
        response.set_cookie("token", token)
        print("time: " + str(datetime.now() - start))
        return response
    return decorator

@app.before_request
def redirect_nonwww():
    e = tldextract.extract(request.url)
    if e.subdomain != 'www' and e.domain == 'slackersnooze':
        return redirect('http://www.slackersnooze.com', code=301)

@app.route("/")
@app.route("/news")
@with_vector
def feed(clicked, seen):
    start = int(request.args.get('p', '0'))*30
    if len(seen) == 0:
        scores = [ doc['score'] for doc in docs() ]
        results = [ (doc, score) for (score, doc) in 
                sorted(zip(scores, docs()), reverse=True) ][start:start+30]
        is_personalized=False
    else:
        VI = la.pinv(np.cov(clicked.T))
        avg = np.sum(clicked, axis=0, dtype=float)/len(seen)
        distances = [ -1*mahalanobis(avg, v, VI=VI) for v in vectors() ]
        results = [(doc, distance) for 
                (distance, doc) in 
                sorted(zip(distances, docs()), reverse=True) if
                not doc['id'] in clicked
                ][start:start+30]
        is_personalized=True
    return app.make_response(render_template("template.html", 
        start=start,
        p=start//30 + 1,
        results=results,
        is_personalized=is_personalized
    ))

@app.route("/docs/<int:doc_id>")
def article(doc_id):
    doc = db.click(doc_id, request.cookies.get('token'))
    return redirect(doc['url'])

@app.route("/docs/<int:doc_id>/comments")
def comments(doc_id):
    doc = db.click(doc_id, request.cookies.get('token'))
    return redirect("https://news.ycombinator.com/item?id="+
            str(doc['id']))

@app.template_filter()
def timesince(dt, default="just now"):
    """
    Returns string representing "time since" e.g.
    3 days ago, 5 hours ago etc.
    """

    now = datetime.utcnow()
    diff = now - dt

    periods = (
        (diff.days // 365, "year", "years"),
        (diff.days // 30, "month", "months"),
        (diff.days // 7, "week", "weeks"),
        (diff.days, "day", "days"),
        (diff.seconds // 3600, "hour", "hours"),
        (diff.seconds // 60, "minute", "minutes"),
        (diff.seconds, "second", "seconds"),
    )

    for period, singular, plural in periods:
        
        if period:
            return "%d %s ago" % (period, singular if period == 1 else plural)

    return default

@app.template_filter()
def hostname(url):
    e = tldextract.extract(url)
    return "{}.{}".format(e.domain, e.suffix)

if __name__ == "__main__":
    p = Process(target=poll, args=(q,))
    p.start()
    db.connect()
    q.put(db.most_recent())
    app.run(host="0.0.0.0", threaded=True)
