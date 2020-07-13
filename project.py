#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday,
                      tms.tm_hour, tms.tm_min, tms.tm_sec,
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

# Q1: replace pass with your code
def extract_email_network(rdd):
    concat_strings = lambda s: (str(s[0]) + ',' + str(s[1]) + ',' + str(s[2]))
    split_string = lambda s: s.split(',')
    val_by_vec = lambda x, t, d: ((x, t[i], d) for i in range(len(t)))
    email_regex = '^[^\s@]+@enron.com$'
    valid_email = lambda s: re.match(email_regex, s) != None
    not_self_loop = lambda t: True if t[0] != t[1] else False
    rdd_mail = rdd.map(lambda x: Parser().parsestr(x))
    rdd_full_email_tuples = rdd_mail.map(lambda x: (x.get('From'),[x.get('To'),x.get('Cc'),x.get('Bcc')],date_to_dt(x.get('Date'))))
    rdd_email_triples = rdd_full_email_tuples.map(lambda x: (x[0], concat_strings(x[1]), x[2])).map(lambda x: (x[0], split_string(x[1]), x[2])).flatMap(lambda x: (val_by_vec(x[0], x[1], x[2])))
    rdd_email_triples = rdd_email_triples.map(lambda x: (x[0], x[1].strip(), x[2]))
    rdd_email_triples_enron = rdd_email_triples.filter(lambda x: valid_email(x[0])).filter(lambda x: valid_email(x[1])).filter(lambda x: not_self_loop((x[0], x[1])))
    distinct_triples = rdd_email_triples_enron.distinct()
    return distinct_triples

# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    if (drange != None):
        rdd_weighted = rdd.filter(lambda x: x[2] <= drange[1] and x[2] >= drange[0]).map(lambda x: (x[0], x[1])).map(lambda w:(w, 1)).reduceByKey(lambda a, b: a + b)
    else:
        rdd_weighted = rdd.map(lambda x: (x[0], x[1])).map(lambda w:(w, 1)).reduceByKey(lambda a, b: a + b)
    return rdd_weighted

# Q3.1: replace pass with your code
def get_out_degrees(rdd):
    rdd_out = rdd.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
    return rdd_out

# Q3.2: replace pass with your code
def get_in_degrees(rdd):
    rdd_in = rdd.map(lambda x: (x[0][1], x[1])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
    return rdd_in

# Q4.1: replace pass with your code
def get_out_degree_dist(rdd):
    rdd_out = rdd.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0]))
    rdd_out_dist = rdd_out.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).sortByKey()
    return rdd_out_dist

# Q4.2: replace pass with your code
def get_in_degree_dist(rdd):
    rdd_in = rdd.map(lambda x: (x[0][1], x[1])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0]))
    rdd_in_dist = rdd_in.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).sortByKey()
    return rdd_in_dist
