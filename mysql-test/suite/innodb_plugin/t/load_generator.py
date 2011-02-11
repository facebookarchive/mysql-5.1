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


def populate_table(con, num_records_before):
  cur = con.cursor()
  for i in xrange(num_records_before):
    cur.execute("INSERT INTO t1(id,msg) VALUES (%d, '%s')" % (i, sha1(i) * 6))
  con.commit()


class Worker(threading.Thread):
  global LG_TMP_DIR

  def __init__(self, num_xactions, xid, con, kill_server, server_pid):
    threading.Thread.__init__(self)
    self.xid = xid
    con.autocommit(False)
    self.con = con
    self.num_xactions = num_xactions
    self.kill_server = kill_server
    self.server_pid = server_pid
    cur = self.con.cursor()
    self.log = cStringIO.StringIO()
    #check if there is a journal entry for this transaction
    cur.execute("SELECT * FROM journal WHERE xid=%d" % self.xid)
    res = cur.fetchone()
    self.seed = None
    self.rand = random.Random()
    self.loop_num = 0
    self.max_id = 1 << 12
    self.num_inserts = 0
    self.num_deletes = 0
    self.num_updates = 0
    self.time_spent = 0
    self.log_f = open('/%s/worker%02d.log' % (LG_TMP_DIR, self.xid), 'a')
    if res is None:
      # this is the first time transaction is running.
      self.seed = self.xid
      self.rand.seed(self.seed)
      cur.execute("""
INSERT INTO
journal(xid, seed, loop_num, num_inserts, num_updates, num_deletes, time_spent)
VALUES (%d, %d, 0, 0, 0, 0, 0.0)""" % (self.xid, self.seed))
      self.con.commit()
    else:
      self.seed = res[1]
      print >> self.log, "set loop_num to ", res[2]
      self.loop_num = res[2]
      self.num_inserts = res[3]
      self.num_updates = res[4]
      self.num_deletes = res[5]
      self.time_spent = res[6]
      self.rand.seed(self.seed)
      for i in xrange(2 * self.loop_num):
        self.rand.randint(0, self.max_id)
#    print "num_inserts=%d,num_updates=%d,num_deletes=%d,time_spent=%d" %\
#(self.num_inserts, self.num_updates, self.num_deletes, self.time_spent)
    self.start()

  def finish(self):
    print >> self.log, "loop_num:%d, total time: %.2f s" % (self.loop_num,
time.time() - self.start_time + self.time_spent)
    print >> self.log_f, self.log.getvalue()
    self.log_f.close()

  def run(self):
    self.start_time = time.time()
    cur = self.con.cursor()
    while self.loop_num < self.num_xactions:
      idx = self.rand.randint(0, self.max_id)
      random_number = self.rand.randint(0, self.max_id) % 3
      msg = (random_number + 4) * sha1("%d%d" % (idx, random_number))
      self.loop_num += 1
      try:
        stmt = None
        if random_number:
          cur.execute("SELECT * FROM t1 WHERE id=%d" % idx)
          res = cur.fetchone()
          if res:
            stmt = "UPDATE t1 SET msg='%s' WHERE id=%d" % (msg, idx)
            self.num_updates += 1
          else:
            stmt = "INSERT INTO t1(id,msg) VALUES (%d,'%s')" % (idx, msg)
            self.num_inserts += 1
        else:
          stmt = "DELETE FROM t1 WHERE id=%d" % idx
          self.num_deletes += 1
        cur.execute(stmt)
        print >> self.log, "Thread %d loop_num %d: %s" % (self.xid,
                                                          self.loop_num,
                                                          stmt)
        cur.execute("""
UPDATE journal
SET loop_num=%d, num_inserts=%d, num_updates=%d, num_deletes=%d, time_spent=%.2f
WHERE xid=%d""" % (self.loop_num,
                   self.num_inserts,
                   self.num_updates,
                   self.num_deletes,
                   time.time() - self.start_time + self.time_spent,
                   self.xid))
        self.con.commit()
      except MySQLdb.Error, e:
        if e.args[0] == 2006:  # server is killed
          print >> self.log,\
"Can not connect mysql server, transaction %d" % self.xid
          self.finish()
          return
    self.finish()
    if self.kill_server:
      print >> self.log, "killing the server"
      os.kill(self.server_pid, signal.SIGKILL)

if  __name__ == '__main__':
  pid_file = sys.argv[1]
  kill_db_after = int(sys.argv[2])
  kill_server = kill_db_after == 0
  num_records_before = int(sys.argv[3])
  num_workers = int(sys.argv[4])
  num_xactions_per_worker = int(sys.argv[5])
  user = sys.argv[6]
  host = sys.argv[7]
  port = int(sys.argv[8])
  db = sys.argv[9]
  server_pid = int(open(pid_file).read())
  workers = []
#  print  "kill_db_after = ",kill_db_after," num_records_before = ", \
#num_records_before, " num_workers= ",num_workers, "num_xactions_per_worker =",\
#num_xactions_per_worker, "user = ",user, "host =", host,"port = ",port,\
#" db = ", db, " server_pid = ", server_pid
  if num_records_before:
    con = MySQLdb.connect(user=user, host=host, port=port, db=db)
    populate_table(con, num_records_before)
    con.close()
  for i in xrange(num_workers):
    con = MySQLdb.connect(user=user, host=host, port=port, db=db)
    worker = Worker(num_xactions_per_worker, i, con, kill_server, server_pid)
    workers.append(worker)
  if kill_db_after:
    time.sleep(kill_db_after)
#    print "killing mysqld"
    os.kill(server_pid, signal.SIGKILL)
  for w in workers:
    w.join()
