#!/usr/bin/env python
import os

def rename_extension():
    for i in os.listdir(os.curdir):
        if i.startswith('.'):
            continue
        base, ext = os.path.splitext(i)
        if ext == '.pybak':
            os.rename(i, ''.join([base, '.py']))

def rename_prefix():
    for i in os.listdir(os.curdir):
        if i.startswith('.'):
            continue
        if i.startswith('test_'):
            continue
        else:
            newname = 'test_%s' % i
            os.rename(i, newname)
