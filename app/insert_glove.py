import db

if __name__ == '__main__':
    with open('glove.840B.300d.txt') as f:
        rows = []
        for i, line in enumerate(f):
            words = line.split(" ")
            word = db.normalize_word(words[0])
            vector = [float(s) for s in words[1:]]
            rows.append((word, vector))
            if len(rows) == 1000:
                db.upsert_word_vectors(rows)
                rows = []
                print(i)
