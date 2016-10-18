import requests
import time
import db
from datetime import datetime

hn_base = "https://hacker-news.firebaseio.com/v0/"
top_stories_url = hn_base + "topstories.json?print=pretty"

def doc_url(id): 
    return hn_base + "item/%s.json?print=pretty.json" % id

def get_doc(id):
    return requests.get(doc_url(id)).json()

def poll(q):
    while True:
        top_posts = requests.get(top_stories_url).json()
        new_posts = set(db.new_doc_ids(top_posts))
        docs = [get_doc(id) for id in top_posts]
        db.upsert_docs(docs)
        db.count_words_from_titles([doc['title'] for doc in docs if doc['id'] in new_posts])
        q.put(db.docs_and_vectors(top_posts))
        print("\nsleeping..."+str(datetime.now()))
         
        time.sleep(5*60)
