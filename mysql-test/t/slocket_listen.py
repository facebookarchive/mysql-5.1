#!/bin/env python
from socket import socket, AF_UNIX, SOCK_DGRAM
from select import select
from os import unlink, getcwd
from os.path import exists
from sys import argv, exit


def main():
  if len(argv) < 2:
    print 'Usage: %s <socket name>' % argv[0]
    exit(1)

  sn = make_relative_path(argv[1])

  s = socket(AF_UNIX, SOCK_DGRAM)
  if len(argv) < 2:
    print 'Usage: %s <socket name>' % argv[0]
    exit(1)

  s.setblocking(False)

  try_unlink(sn)
  s.bind(sn)

  print 'listening on %r' % sn

  while not exists('slocket_listen_kill_flag'):
    x,_,_ = select([s], [], [], 1.0)
    if len(x) > 0:
      x = x[0]
      y = x.recv(1024)
      print y

  print 'found slocket_listen_kill_flag... closing'
  s.close()
  try_unlink(sn)

def make_relative_path(path):
  current_dir = getcwd()
  if path.find(current_dir) == 0:
    path = path[len(current_dir):]
    if path[0] == '/':
      path = path[1:]
  return path

def try_unlink(sn):
  try:
    unlink(sn)
  except:
    pass

if __name__ == '__main__':
  main()
