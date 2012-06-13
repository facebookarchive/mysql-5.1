import hashlib
import MySQLdb
import os
import random
import signal
import sys
import threading
import time

class Worker(threading.Thread):
  global TEST_TMP_DIR

  def __init__(self, xid, con):
    threading.Thread.__init__(self)
    self.xid = xid
    con.autocommit(True)
    self.con = con
    cur = self.con.cursor()
    self.num_inserts = 0
    self.num_queries = 0
    self.log = open('/%s/worker%02d.log' % (TEST_TMP_DIR, self.xid), 'a')

  def finish(self):
    print >> self.log, "%d inserts, %d queries, total time: %.2f s" % (
        self.num_inserts, self.num_queries, time.time() - self.start_time)
    self.log.close()

  def run(self):
    try:
      self.runme()
    except Exception, e:
      print >> self.log, "caught (%s)" % e
    finally:
      self.finish()

  def runme(self):
    global shutdown_now
    global is_read_only
 
    self.start_time = time.time()
    print >> self.log, "thread %d started" % self.xid

    while not shutdown_now:
      try:
        stmt = None

        cur = self.con.cursor()

        if not is_read_only:
          stmt = "INSERT INTO t1 (p,worker,count) VALUES (NULL,%d,%d)" % (
              self.xid, self.num_inserts)
          self.num_inserts += 1
        else:
          stmt = "SELECT * FROM t1 WHERE p < %d ORDER BY p DESC LIMIT 10" % (
              self.num_inserts)
          self.num_queries += 1

        cur.execute(stmt)
        cur.close()

        if ((self.num_inserts + self.num_queries) % 1000) == 0:
          print >> self.log, "worker %d did %d inserts and %d queries" % (
              self.xid, self.num_inserts, self.num_queries)

      except MySQLdb.Error, e:
        print >> self.log, "mysql error for stmt(%s) %s" % (stmt, e)

class Setter(threading.Thread):

  def __init__(self, con):
    global TEST_TMP_DIR
    threading.Thread.__init__(self)
    con.autocommit(True)
    self.con = con
    self.num_sets = 0
    self.log = open('/%s/setter.log' % TEST_TMP_DIR, 'a')

  def finish(self):
    print >> self.log, "%d sets, total time: %.2f s" % (
        self.num_sets, time.time() - self.start_time)
    self.log.close()

  def run(self):
    try:
      self.runme()
    except Exception, e:
      print >> self.log, "Setter caught (%s)" % e
    finally:
      self.finish()

  def runme(self):
    global shutdown_now
    global is_read_only

    print >> self.log, "run for %d seconds" % test_seconds
    self.start_time = time.time()

    while not shutdown_now:
      time.sleep(2);

      try:
        cursor = self.con.cursor()
        s = time.time()
        cursor.execute("set global read_only=0")
        e = time.time()
        print >> self.log, "disable read only: %f seconds" % (e - s)
        self.num_sets += 1
      except MySQLdb.Error, e:
        print >> self.log, "mysql error for stmt set read only: %s" % e

      is_read_only = False  

      time.sleep(2)
      is_read_only = True

      try:
        cursor = self.con.cursor()
        s = time.time()
        cursor.execute("set global read_only=1")
        e = time.time()
        print >> self.log, "enable read only: %f seconds" % (e - s)
      except MySQLdb.Error, e:
        print >> self.log, "mysql error for stmt set read only: %s" % e


if  __name__ == '__main__':
  global TEST_TMP_DIR
  global shutdown_now
  global is_read_only

  is_read_only = True
  shutdown_now = False

  pid_file = sys.argv[1]
  TEST_TMP_DIR = sys.argv[2]
  num_workers = int(sys.argv[3])
  test_seconds = int(sys.argv[4])
  user = sys.argv[5]
  host = sys.argv[6]
  port = int(sys.argv[7])
  db = 'test'
  workers = []
  server_pid = int(open(pid_file).read())
  log = open('/%s/main.log' % TEST_TMP_DIR, 'a')

  print >> log, "start %d threads" % num_workers
  for i in xrange(num_workers):
    worker = Worker(i,
                    MySQLdb.connect(user=user, host=host, port=port, db=db))
    worker.start()
    workers.append(worker)

  setter = Setter(MySQLdb.connect(user=user, host=host, port=port, db=db))
  setter.start() 

  time.sleep(test_seconds)
  shutdown_now = True

  print >> log, "wait for threads"
  for w in workers:
    w.join()

  print >> log, "wait for setter"
  setter.join()

  print >> log, "all threads done"
  log.close()

