'''
zfsobj   [zfsobj_id, uid, gid, ..., size, trusted_lov, trusted_link]
name     [fid, name, parent_fid, pk(name, parent_fid), index(fid)]
metadata [fid, uid, gid, ..., size]

todo: integrate with zester

get_entries_for_path(path):
    fid=path_to_fid()
    # recur into .. join on mode in metadata .. basically listdir

get_entries_for_uid_in_path(uid, path):
    entries = []
    for fid in get_fids_for_uid(uid):
       for path in fid_to_path(fid):
          etnries.append(path)
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
