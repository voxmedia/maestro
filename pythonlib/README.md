
### Maestro Python Client ###

The easiest way to use the latest client code is to import it from
Maestro as follows (there is no packages to install):

``` python
>>> import requests # $ pip install requests
>>>
>>> url = "https://maestro.example.com"
>>> token = "<your API token>"
>>> exec(requests.get(url+"/py/maestro.py", headers={"X-Api-Token" : token}).content)
>>>
>>> # we now have a Maestro class, let's read the documentation:
>>> help(Maestro)
Help on class Maestro in module __main__:

class Maestro(__builtin__.object)
 |  Access to a Maestro instance.
 ...

```

Example usage of a non-external table (i.e. any table except external):

``` python
import requests # $ pip install requests

url = "https://maestro.example.com"
token = "<your API token>"
exec(requests.get(url+"/py/maestro.py", headers={"X-Api-Token" : token}).content)

if __name__ == "__main__":

    dest = '/var/tmp/foo'

    m = Maestro(url, token)

    table_id = 193
    with m.Table(table_id, wait=True, gcs_fetch_path=dest) as t:

        # do stuff
        print("Hello from within:", t)

        # alternatively to the gcs_fetch_path argument above:
        #files = t.gcs_fetch(dest)

        # alternatively, we can read all the data in sequence without
        # ever storing it in a file like this:
        # lr = t.reader()
        # line = lr.readline()
        # while line:
        #    # do something

        print("Downloaded files:")
        for path in t.files():
            print("  %s" % path)

```

This will look like this:

``` sh
$ python test.py
Waiting for table maestro_stage.foo
Fetching maestro_stage_foo_1541691350_000000000000.csv.gz into /var/tmp/foo...
Transferred 11745 bytes in 0.254401s (46167.277062 B/s).
Hello from within: <maestro.Table object at 0x7f6b5685c898>
Downloaded files:
  /var/tmp/foo/maestro_stage_foo_1541691350_000000000000.csv.gz

```

Example usage of an external table (i.e. we are providing data for it):

``` python
import requests # $ pip install requests

url = "https://maestro.example.com/"
token = "<your API token>"
exec(requests.get(url+"/py/maestro.py", headers={"X-Api-Token" : token}).content)

if __name__ == "__main__":

    source = '/var/tmp/foo.json'

    m = Maestro(url, token)

    table_id = "maestro_stage.foo"
    with m.Table(table_id, wait=True) as t:

        # do stuff
        print("Hello from within:", t)

        t.gcs_upload(source)

        print("Done.")

```

This will look like this:

``` sh
$ python test.py
Waiting for external table maestro_stage.foo to start running...
Uploading /var/tmp/foo.json into https://storage.googleapis.com/maestro_stage/foo_1541700897.json...
Transferred 2294326 bytes in 1.785644s (1284873.093453 B/s).
Starting BigQuery load job for foo...
BigQuery load job finished OK.
Done.

```

### A long and complex example ###

This is an example of a recommendation using the Apple Turi library.

We have four Maestro tables here:

``` sql
-- table_id maestro_stage.recs_user_items
-- This table provides a list of user_id, article_id and publish_date

SELECT user_id, article_id, published_date
  FROM (
    SELECT
        date
        ,visitId
        ,fullVisitorId as user_id
        , MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) AS article_id
        FROM TABLE_DATE_RANGE([voxmedia.com:example-project:12345678.ga_sessions_],
                   DATE_ADD(CURRENT_DATE(), -5, "DAY"),
                   DATE_ADD(CURRENT_DATE(), -1, "DAY"))
        WHERE hits.type='PAGE'
          AND hits.page.hostname = "www.example.com"
        GROUP BY 1,2,3
  ) ga
  JOIN (
    SELECT STRING(id) AS id, published_date
      FROM [vox-data-lake:chorus_db_prod.entries]
     WHERE type = 'Article'
       AND published_date IS NOT NULL
  ) a
  ON ga.article_id = a.id
```

``` sql
-- table_id maestro_stage.recs_output is an _external_ table, we upload data to it, there is no SQL.

```

``` sql
-- table_id maestro_stage.recs_deploy
-- Data for final deploy of recommendations. Here we add title and slug.

WITH recommends AS (
  SELECT recs.article_id, recs.recs
    FROM `vox-data-lake.maestro_stage.recs_output`
         ,UNNEST(recs) AS recs
)
SELECT article_id, rank, similar, slug, title
  FROM (
    SELECT article_id, rank+1 AS rank, recs.similar
      FROM recommends
           , UNNEST(recs) AS recs WITH OFFSET AS rank
  ) r
  JOIN `vox-data-lake.chorus_db_prod.entries` e
    ON r.similar = e.id
ORDER BY article_id, rank
```

``` sql
-- maestro_stage.recs_archive
-- This table will archive the recommendations
SELECT CURRENT_TIMESTAMP() AS time, t.*
  FROM `vox-data-lake.maestro_stage.recs_output` t

```

And here is the Python code, all in one file. It is best to read it
from the bottom where `__main__` begins.

``` python

import sys
import json
import math
import time
import os
from datetime import datetime

# Apple Turi (formerly known as Graphlab)
import turicreate as gl
import turicreate.aggregate as agg

import requests # $ pip install requests

# The URL to Maestro.
MAESTRO_URL = "https://maestro.example.com/"

# The Auth token which can be obtained from the Maestro UI.
# Note that no other credentials are necessay for this to work,
# you do not need GCS or BigQuery access.
MAESTRO_TOKEN = "AQAAAA ... =="

exec(requests.get(MAESTRO_URL+"/py/maestro.py", headers={"X-Api-Token" : MAESTRO_TOKEN}).content)

def save_recs(dest, article_ids, sf_recommend, version, score_column='score', k=5):

    # Generate and save all the recommendations in one large json
    # newline-delimited file (this is the format that BigQuery
    # expects), the path to which is the return value of this
    # function.

    recs_dir = os.path.join(dest, "recs")
    if not os.path.exists(recs_dir):
        os.makedirs(recs_dir)

    recs_path = os.path.join(recs_dir, "recs.json")

    print("  Saving recommendations in", recs_path)
    with open(recs_path, 'w', encoding="utf8") as out:
        row = {"version": version, "recs": []}
        for article_id in article_ids:

            article_filter = sf_recommend['article_id'] == article_id
            rec = sf_recommend[article_filter].sort(score_column, ascending=False).head(k)

            if rec.num_rows() >= k:
                recs = [ {"similar":int(r['similar'])} for r in rec ]
                row["recs"].append({'article_id': int(article_id), 'recs': recs})

        json.dump(row, out)
        out.write("\n")

    return recs_path

def apply_time_decay(sf_recommend, half_life=10*24*3600):

    def _decay(row, half_life=half_life):
        diff = (row['published_date_similar'] - row['published_date']).total_seconds()
        decay = math.e**(-abs(diff)/half_life)
        return decay*row['score']

    sf_recommend['score'] = sf_recommend.apply(_decay)
    sf_recommend.remove_column('rank') # avoid confusion
    return sf_recommend

def gen_recs(dest, version, limit_articles=0, k=5):

    # select_article_ids
    sf_articles = gl.load_sframe(os.path.join(dest, "sf_articles"))
    print ("  Number of articles: %s" % sf_articles.num_rows())

    # load model
    model = gl.load_model(os.path.join(dest, "model"))

    article_ids = sf_articles["article_id"]
    if limit_articles > 0:
        article_ids = article_ids[:limit_articles]

    ## generate recs for all articles at once
    sf_recommend = model.get_similar_items(article_ids, k=k)
    #print(sf_recommend)
    #
    # +------------+----------+-----------------------+------+
    # | article_id | similar  |         score         | rank |
    # +------------+----------+-----------------------+------+
    # |  17181849  | 16094701 |  0.007936537265777588 |  1   |
    # |  17181849  | 17278503 |  0.007142841815948486 |  2   |

    # join to get similar article published_date
    sf_recommend = sf_recommend.join(sf_articles, on={'similar':'article_id'})
    sf_recommend = sf_recommend.rename({'published_date':'published_date_similar','count':'count_similar'})

    # join to get originnal article publish time and subtract
    sf_recommend = sf_recommend.join(sf_articles, on='article_id')

    print("  Applying time decay...")
    sf_recommend = apply_time_decay(sf_recommend)

    print("  Generating recommendations...")
    return save_recs(dest, article_ids, sf_recommend, version, k=k)

def exclude_low_pageviews(sf, sf_articles, pageview_min=20):
    cnt_before, int_before = sf_articles.num_rows(), sf.num_rows()
    pageview_filter = sf_articles['count'] <= pageview_min
    exclude_articles = sf_articles[pageview_filter]['article_id']
    sf_articles = sf_articles[pageview_filter == 0]
    sf = sf.filter_by(exclude_articles, "article_id", exclude=True)
    cnt_after, int_after = sf_articles.num_rows(), sf.num_rows()
    print("  Excluded %d items, %d user-item interactions." % (cnt_before-cnt_after, int_before-int_after))
    return sf, sf_articles

def gen_model(dest):

    # This code manipulates the data, generates the model using
    # graphlab, then saves the model and the list of articles in
    # subdirectories of dest.

    sf = gl.SFrame.read_csv(dest, column_type_hints=str)
    sf_articles = sf.groupby(['article_id', 'published_date'], operations={'count': agg.COUNT()})  # just the articles

    print("  Excluding low pageviews...")
    sf, sf_articles = exclude_low_pageviews(sf, sf_articles, 20)

    print("  Converting article published_date...")
    sf_articles['published_date'] = sf_articles['published_date'].apply(lambda x: datetime.strptime(x[:16], '%Y-%m-%d %H:%M'))

    print("  Running the item similarity thing...")
    model = gl.item_similarity_recommender.create(sf, 'user_id', 'article_id')

    print("  Saving model and articles sframe...")
    model.save(os.path.join(dest, "model"))
    sf_articles.save(os.path.join(dest, "sf_articles"))
    print("  Done.")

def deploy(deploy_path, url_prefix):

    sf = gl.SFrame.read_csv(deploy_path, column_type_hints=str)

    sf_articles = sf.groupby(['article_id'], operations={})

    for article_id in sf_articles['article_id'][:1]: # only first
        output = {"article_id":article_id, "recs":[]}
        recs = sf[sf['article_id'] == article_id]
        for rec in recs:
            output["recs"].append({"title":rec["title"], "url":url_prefix+rec["slug"]})

        # TODO: this would be written to some kind of a file
        print(json.dumps(output, indent=4, separators=(',', ': ')))


if __name__ == "__main__":

    # The location on disk where we want all this data to live. This
    # directory MUST exist, Maestro will not create it.
    dest = "/home/example/data"

    # Algorithm version. This should change any time the code above changes.
    VERSION = "example_123"

    # Create a Maestro client
    m = Maestro(MAESTRO_URL, MAESTRO_TOKEN)

    print("** Generating model...")

    # The "with" block below will wait for the table to finish. This
    # is done by periodically checking the table status and watching
    # for the last successful run time to change. As soon as that
    # happens, all the GCS export CSV files are downloaded into dest
    # directory. If wait was set to false, then the download would
    # start immediately and then the inner part of the block is
    # executed.
    #
    # At the end of block, all the files downloaded from GCS are going
    # to be deleted because cleanup=True.

    with m.Table("maestro_stage.recs_user_items", wait=True, gcs_fetch_path=dest, cleanup=True) as t:

        # Generate and save the model and article list.
        gen_model(dest)

    # At this point the GCS file(s) are deleted.

    print("** Generating recommendations from our model....")

    # Load the article list and the model and generate
    # recommendations. (NB: We didn't have to load and save the model
    # and articles, but this is convenient if we want to restart this
    # process from this step).

    recs_path = gen_recs(dest, VERSION, limit_articles=100)


    print("** Uploading generated recommendations to BigQuery....")
    with m.Table("maestro_stage.recs_output", cleanup=True) as t:
        t.gcs_upload(recs_path)


    print("** Deploying recommendations from BQ to S3 (not really)...")
    with m.Table("maestro_stage.recs_deploy", cleanup=True) as t:

        deploy_path = os.path.join(dest, "deploy")
        if not os.path.exists(deploy_path):
            os.makedirs(deploy_path)

        t.gcs_fetch(deploy_path)

        # This function would deploy this to S3, but since that
        # actually would require credentials it just formats them as
        # JSON and prints the first one out.

        deploy(deploy_path, "https://www.example.com/")

```
