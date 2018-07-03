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
