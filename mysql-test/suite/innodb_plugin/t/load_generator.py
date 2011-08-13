import cStringIO
import hashlib
import MySQLdb
import os
import random
import signal
import sys
import threading
import time

LG_TMP_DIR = '/tmp/load_generator'


def sha1(x):
  return hashlib.sha1(str(x)).hexdigest()


def populate_table(con, num_records_before, do_blob, log):
  cur = con.cursor()

  if not do_blob:
    for i in xrange(num_records_before):
      cur.execute("INSERT INTO t1(id,msg) VALUES (NULL, '%s')" % (sha1(i) * 6))
  else:
    for i in xrange(num_records_before):
      cur.execute("""
INSERT INTO t1(id,x1,x2,x3,msg) VALUES (NULL, 'x1', 'x2', LPAD('z', %d, 'x'), '%s')
""" % (random.randint(1, 9000), sha1(i) * 6))

  con.commit()


class Worker(threading.Thread):
  global LG_TMP_DIR

  def __init__(self, num_xactions, xid, con, server_pid, do_blob, max_id):
    threading.Thread.__init__(self)
    self.do_blob = do_blob
    self.xid = xid
    con.autocommit(False)
    self.con = con
    self.num_xactions = num_xactions
    cur = self.con.cursor()
    self.rand = random.Random()
    self.rand.seed(xid * server_pid)
    self.loop_num = 0
    self.max_id = max_id
    self.num_inserts = 0
    self.num_deletes = 0
    self.num_updates = 0
    self.time_spent = 0
    self.log = open('/%s/worker%02d.log' % (LG_TMP_DIR, self.xid), 'a')
#    print "num_inserts=%d,num_updates=%d,num_deletes=%d,time_spent=%d" %\
#(self.num_inserts, self.num_updates, self.num_deletes, self.time_spent)
    self.start()

  def finish(self):
    print >> self.log, "loop_num:%d, total time: %.2f s" % (
        self.loop_num, time.time() - self.start_time + self.time_spent)
    self.log.close()

  def run(self):
    try:
      if not do_blob:
        self.runme()
        print >> self.log, "ok, no blob"
      else:
        self.runme_blob()
        print >> self.log, "ok, blob"
    except Exception, e:
      print >> self.log, "caught (%s)" % e
    finally:
      self.finish()

  def runme(self):
    self.start_time = time.time()
    cur = self.con.cursor()
    print >> self.log, "thread %d started, run from %d to %d" % (
        self.xid, self.loop_num, self.num_xactions)

    while not self.num_xactions or (self.loop_num < self.num_xactions):
      idx = self.rand.randint(0, self.max_id)
      insert_or_update = self.rand.randint(0, 3)
      msg = sha1("%d%d" % (idx, self.loop_num))
      self.loop_num += 1
      try:
        stmt = None
        if insert_or_update:
          cur.execute("SELECT * FROM t1 WHERE id=%d" % idx)
          res = cur.fetchone()
          if res:
            if self.rand.randint(0, 1):
              stmt = "UPDATE t1 SET msg='%s' WHERE id=%d" % (msg, idx)
            else:
              stmt = "INSERT INTO t1 (msg,id) VALUES ('%s', %d) ON DUPLICATE KEY UPDATE msg=VALUES(msg), id=VALUES(id)" % (msg, idx)
            self.num_updates += 1
          else:
            r = self.rand.randint(0, 2)
            if r == 0:
              stmt = "INSERT INTO t1(id,msg) VALUES (%d,'%s')" % (idx, msg)
            elif r == 1:
              stmt = "INSERT INTO t1 (msg,id) VALUES ('%s', %d) ON DUPLICATE KEY UPDATE msg=VALUES(msg), id=VALUES(id)" % (msg, idx)
            else:
              stmt = "INSERT INTO t1 (msg,id) VALUES ('%s', NULL)" % msg
            self.num_inserts += 1
        else:
          stmt = "DELETE FROM t1 WHERE id=%d" % idx
          self.num_deletes += 1

        query_result = cur.execute(stmt)
        if (self.loop_num % 100) == 0:
          print >> self.log, "Thread %d loop_num %d: result %d: %s" % (self.xid,
                                                            self.loop_num, query_result,
                                                            stmt)

        # 30% commit, 10% rollback, 60% don't end the trx
        r = self.rand.randint(1,10)
        if r < 4:
          self.con.commit()
        elif r == 4:
          self.con.rollback()

      except MySQLdb.Error, e:
        if e.args[0] == 2006:  # server is killed
          print >> self.log, "mysqld down, transaction %d" % self.xid
          return
        else:
          print >> self.log, "mysql error for stmt(%s) %s" % (stmt, e)

    try:
      self.con.commit()
    except Exception, e:
      print >> self.log, "commit error %s" % e

  def runme_blob(self):
    self.start_time = time.time()
    cur = self.con.cursor()
    while not self.num_xactions or (self.loop_num < self.num_xactions):
      idx = self.rand.randint(0, self.max_id)
      insert_or_update = self.rand.randint(0, 3)
      msg = sha1("%d%d" % (idx, self.loop_num))
      self.loop_num += 1
      try:
        stmt = None
        if insert_or_update:
          cur.execute("SELECT * FROM t1 WHERE id=%d" % idx)
          res = cur.fetchone()
          if res:
            r = self.rand.randint(0,3)
            if r == 0:
              stmt = "UPDATE t1 SET msg='%s', x1=LPAD('a', %d, 'b') WHERE id=%d" % (
                      msg, self.rand.randint(1, 9000), idx)
            elif r == 1:
              stmt = "UPDATE t1 SET msg='%s', x2=LPAD('a', %d, 'b') WHERE id=%d" % (
                      msg, self.rand.randint(1, 9000), idx)
            elif r == 2:
              stmt = """
INSERT INTO t1 (msg, id, x1) VALUES ('%s', %d, LPAD('a', %d, 'b'))
ON DUPLICATE KEY UPDATE msg=VALUES(msg), id=VALUES(id), x1=VALUES(x1)""" % (
msg, idx, self.rand.randint(1, 9000))
            elif r == 3:
              stmt = """
INSERT INTO t1 (msg, id, x2) VALUES ('%s', %d, LPAD('a', %d, 'b'))
ON DUPLICATE KEY UPDATE msg=VALUES(msg), id=VALUES(id), x2=VALUES(x2)""" % (
msg, idx, self.rand.randint(1, 9000))
            self.num_updates += 1
          else:
            r = self.rand.randint(0,2)
            if r == 0:
              stmt = """
INSERT INTO t1(id,x1,x2,x3,msg) VALUES (%d, LPAD('1',%d,'2'), LPAD('2',%d,'3'), LPAD('3',%d,'4'), '%s')
""" % (idx, self.rand.randint(1, 9000), self.rand.randint(1, 9000), self.rand.randint(1, 9000), msg)
            elif r == 1:
              stmt = """
INSERT INTO t1(id,x1,x2,x3,msg) VALUES (%d, LPAD('1',%d,'2'), LPAD('2',%d,'3'), LPAD('3',%d,'4'), '%s')
ON DUPLICATE KEY UPDATE msg=VALUES(msg), id=VALUES(id), x1=VALUES(x1), x2=VALUES(x2), x3=VALUES(x3)
""" % (idx, self.rand.randint(1, 9000), self.rand.randint(1, 9000), self.rand.randint(1, 9000), msg)
            else:
              stmt = """
INSERT INTO t1(id,x1,x2,x3,msg) VALUES (NULL, LPAD('1',%d,'2'), LPAD('2',%d,'3'), LPAD('3',%d,'4'), '%s')
""" % (self.rand.randint(1, 9000), self.rand.randint(1, 9000), self.rand.randint(1, 9000), msg)

            self.num_inserts += 1
        else:
          stmt = "DELETE FROM t1 WHERE id=%d" % idx
          self.num_deletes += 1
        cur.execute(stmt)

        if (self.loop_num % 100) == 0:
          print >> self.log, "Thread %d loop_num %d: %s" % (self.xid,
                                                            self.loop_num,
                                                            stmt)

        # 30% commit, 10% rollback, 60% don't end the trx
        r = self.rand.randint(1,10)
        if r < 4:
          self.con.commit()
        elif r == 4:
          self.con.rollback()

      except MySQLdb.Error, e:
        if e.args[0] == 2006:  # server is killed
          print >> self.log, "mysqld down, transaction %d" % self.xid
          return
        else:
          print >> self.log, "mysql error for stmt(%s) %s" % (stmt, e)

    try:
      self.con.commit()
    except Exception, e:
      print >> self.log, "commit error %s" % e


if  __name__ == '__main__':
  pid_file = sys.argv[1]
  kill_db_after = int(sys.argv[2])
  num_records_before = int(sys.argv[3])
  num_workers = int(sys.argv[4])
  num_xactions_per_worker = int(sys.argv[5])
  user = sys.argv[6]
  host = sys.argv[7]
  port = int(sys.argv[8])
  db = sys.argv[9]
  do_blob = int(sys.argv[10])
  max_id = int(sys.argv[11])
  workers = []
  server_pid = int(open(pid_file).read())
  log = open('/%s/main.log' % LG_TMP_DIR, 'a')

#  print  "kill_db_after = ",kill_db_after," num_records_before = ", \
#num_records_before, " num_workers= ",num_workers, "num_xactions_per_worker =",\
#num_xactions_per_worker, "user = ",user, "host =", host,"port = ",port,\
#" db = ", db, " server_pid = ", server_pid

  if num_records_before:
    print >> log, "populate table do_blob is %d" % do_blob
    con = MySQLdb.connect(user=user, host=host, port=port, db=db)
    populate_table(con, num_records_before, do_blob, log)
    con.close()

  print >> log, "start %d threads" % num_workers
  for i in xrange(num_workers):
    worker = Worker(num_xactions_per_worker, i,
                    MySQLdb.connect(user=user, host=host, port=port, db=db),
                    server_pid, do_blob, max_id)
    workers.append(worker)

  if kill_db_after:
    print >> log, "kill mysqld"
    time.sleep(kill_db_after)
    os.kill(server_pid, signal.SIGKILL)

  print >> log, "wait for threads"
  for w in workers:
    w.join()

  print >> log, "all threads done"

