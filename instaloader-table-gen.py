#!/usr/bin/env python3

import logging
import csv, os, lzma, json, sys
from datetime import datetime

if len(sys.argv) > 1:
    folder = os.path.abspath(sys.argv[1])
else:
    sys.exit("Missing folder argument")

basefolder = os.path.join(folder, "..")
query = os.path.basename(folder)

logging.info("Processing files for " + folder)

outfn = os.path.join(basefolder, query + "_posts.csv")
outfields = [
    'postid','date','userid','username','userfullname',
    'thumbnail_url', 'is_video', 'post_url', 'num_images',
    'accessibility_caption','caption','hashtags',
    'likecount', 'commentcount']
outf = open(outfn, "w")
outcsv = csv.DictWriter(outf, outfields, escapechar="\\", quoting=csv.QUOTE_ALL)
outcsv.writeheader()

# Prepare hashtag stats CSV
outhashstatsfn = os.path.join(basefolder, query + "_hashtag_stats.csv")
outhashstatsfields = ['hashtag','count', 'likecount', 'commentcount', 'engagementcount', 'averagelikes', 'averagecomments', 'averageengagement']
outhashstatsf = open(outhashstatsfn,"w")
outhashstatscsv = csv.DictWriter(outhashstatsf,outhashstatsfields,escapechar="\\", quoting=csv.QUOTE_ALL)
outhashstatscsv.writeheader()

# Prepare account stats CSV
outaccstatsfn = os.path.join(basefolder, query + 'account_stats.csv')
outaccstatsfields = ['username', 'userfullname', 'userid', 'count', 'likecount', 'commentcount', 'engagementcount', 'averagelikes', 'averagecomments','averageengagement']
outaccstatsf = open(outaccstatsfn, "w")
outaccstatscsv = csv.DictWriter(outaccstatsf, outaccstatsfields, escapechar='\\', quoting=csv.QUOTE_ALL)
outaccstatscsv.writeheader()

# Prepare hashtags CSV
outhashfn = os.path.join(basefolder, query + "_hashtags.csv")
outhashfields = ['post', 'hashtag']
outhashf = open(outhashfn, 'w')
outhashcsv = csv.DictWriter(outhashf, outhashfields,escapechar="\\", quoting=csv.QUOTE_ALL)
outhashcsv.writeheader()

# Prepare images CSV

hashstats={}
accstats = {}

infns = os.listdir(folder)

if len(infns)==0:
    logging.info("No files found in folder")

# Number of processed files
numproc = 0

# For each file in folder
for infn in infns:
    outrow = {}

    # If file is caption
    if infn.endswith(".json.xz"):
        numproc +=1
        print(numproc, end=" ")
        sys.stdout.flush()

        try:
            metadata = json.loads(lzma.open(os.path.join(folder, infn)).read().decode('utf8'))
        except Exception as exc:
            logging.exception(exc)
            continue

        try:
            caption = metadata['node']['edge_media_to_caption']['edges'][0]['node']['text'].replace("\n", " ")
        except Exception:
            caption = ""

        try:
            accessibility_caption = metadata['node']['accessibility_caption']
        except Exception:
            accessibility_caption = "NONE"

        timestamp = int(metadata["node"]["taken_at_timestamp"])
        postdate = datetime.fromtimestamp(timestamp).strftime('%d/%m/%Y %H:%M:%S')

        postid = metadata["node"]["shortcode"]
        userid = metadata["node"]["owner"]["id"]

        posturl = "https://www.instagram.com/p/" + postid

        likecount = int(metadata["node"]["edge_media_preview_like"]["count"])
        commentcount = int(metadata['node']['edge_media_to_comment']['count'])

        username = metadata['node']['owner']['username']
        userfullname = metadata['node']['owner']['full_name']

        thumbnail_url = metadata['node']['thumbnail_src']
        is_video = metadata['node']['is_video']

        if not 'edge_sidecar_to_children' in metadata['node']:
            num_images=1
        else:
            images = metadata['node']['edge_sidecar_to_children']['edges']
            num_images = len(images)
        
        # Hashtags
        words = caption.split(" ")
        hashtags= []
        for word in words:
            if word.startswith("#"):
                hashtags.append(word.lower())
        for hashtag in hashtags:
            outhashcsv.writerow({'post':postid, 'hashtag':hashtag})
            if not hashtag in hashstats:
                hashstats[hashtag] = {'count': 1, 'likecount': likecount, 'commentcount': commentcount}
            else:
                hashstats[hashtag]['count'] += 1
                hashstats[hashtag]['likecount'] += likecount
                hashstats[hashtag]['commentcount'] += commentcount
        
        if not username in accstats:
            accstats[username] = {'count': 1, 'likecount': likecount, 'commentcount': commentcount, 'userfullname': userfullname, 'userid': userid}
        else:
            accstats[username]['count'] += 1
            accstats[username]['likecount'] += likecount
            accstats[username]['commentcount'] += commentcount

        outrow['postid'] = postid
        outrow['date'] = postdate
        outrow['userid'] = userid
        outrow['username'] = username
        outrow['userfullname'] = userfullname
        outrow['thumbnail_url'] = thumbnail_url
        outrow['is_video'] = is_video
        outrow['post_url'] = posturl
        outrow['accessibility_caption'] = accessibility_caption
        outrow['num_images'] = num_images
        outrow['caption'] = caption
        outrow['hashtags'] = ",".join(hashtags)
        outrow['likecount'] = likecount
        outrow['commentcount'] = commentcount
        outcsv.writerow(outrow)

for hashtag, info in hashstats.items():
    engagementcount = 2*info['commentcount']+info['likecount']
    outhashstatscsv.writerow({
        'hashtag':hashtag,
        'count':info['count'],
        'likecount': info['likecount'],
        'commentcount': info['commentcount'],
        'engagementcount': engagementcount,
        'averagelikes': info['likecount']/info['count'],
        'averagecomments': info['commentcount']/info['count'],
        'averageengagement': engagementcount/info['count']
        })

for account, info in accstats.items():
    engagementcount = 2*info['commentcount']+info['likecount']
    outaccstatscsv.writerow({
        'username': account,
        'userfullname': info['userfullname'],
        'userid': info['userid'],
        'count':info['count'],
        'likecount': info['likecount'],
        'commentcount': info['commentcount'],
        'engagementcount': engagementcount,
        'averagelikes': info['likecount']/info['count'],
        'averagecomments': info['commentcount']/info['count'],
        'averageengagement': engagementcount/info['count']
    })


logging.info('Done.')