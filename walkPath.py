import os,sys

def walk_path(dpath, dfile):
	if not os.path.exists(dpath):
		print '\t %s does not exists.' % dpath
		return -1

	dtuple = [dirs for dirs in os.walk(dpath)][0]
	root = dtuple[0]

	if root[len(root)-1] != os.seq:
		root += os.seq

	f = file(dfile, 'w')
	for files in dtuple[2]:
		f.write('%s\n' % (root + files))
		f.flush()

	f.close()

	for dirs in dtuple[1]:
		walk_path(root + dirs, dfile)

	return 1