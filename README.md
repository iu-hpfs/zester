# zester

Zester is a work in progress project to enable Lester-like functionality for Lustre ZFS using ZDB, as well as generating metadata (including size) for a Lustre filesystem by assembling the ZDB data available from MDT and OST ZDB dumps.

We will be updating this repository with more information moving forward, but for the immediate future the slides from the "Roads to Zester" LUG 17 talk included in this repository are the best introduction to Zester and how it works.

In order to run Zester, first run the standard ZFS ZDB tool against each MDT and OST dataset on your filesystem:

zdb -dddd <dataset> > filename.zdb

Saving the output as follows for each MDT and OST on your filesystem:

for MDT: mdt_<mdtidx>.zdb
for OST: ost_<ostidx>.zdb

The filename is important, as it is what Zester uses to identify the ZDB dump type as well as MDT and OST indexes.

NOTE: MDT and OST indexes must be specified in decimal

Once all of the zdb dumps are available, run Zester with all of the MDT and OST ZDB generated dumps:

python2 zester mdt_0.zdb ost_0.zdb ost_1.zdb

This will parse all ZDB dumps into SQLite representations, then use those SQLite databases to assemble metadata about the Lustre file system (including size) and generate a 'metadata.db' SQLite database that can be queried against.

NOTE: Only regular files are currently processed. Directories will be supported soon.

NOTE: The generated metadata DB currently stores the path in the FID column as MDT FID generation is experimental. Please see the presentaiton for currently used columns.

We have also included scripts for test file generation, canonical metadata DB generation, and comparison. We will add more information on these scripts in the coming days.
