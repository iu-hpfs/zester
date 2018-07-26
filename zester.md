'''
zfsobj   [zfsobj_id, uid, gid, ..., size, trusted_lov, trusted_link]
name     [fid, name, parent_fid, pk(name, parent_fid), index(fid)]
metadata [fid, uid, gid, ..., size]
'''

## tables

metadata
  * path (text primary key)
  * uid (integer)
  * gid (integer)
  * ctime (integer)
  * mtime (integer)
  * atime (integer)
  * mode (integer)
  * type (integer)
  * size (integer)
  * fid (text)
  * (idx (fid))

* names
  * id (autogen pk)
  * fid (text)
  * name (text)
  * parent_fid (text)
  * (idx (name, parent_fid))
  * (idx (fid)
  * (idx (parent_fid)

* paths
  * path (text primary key)
  * fid (text)
  * (idx (fid))
