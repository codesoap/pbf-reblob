pbf-reblob allows you to easily increase the blob size within open
streetmap's PBF files. This reduces the size of PBF files. It works best
with small extracts.

# Demo
Here are the results of file sizes reduced with pbf-reblob; zstd
compression seems to work better than the default zlib compression with
larger blobs:

![graph of achieved compression ratios](https://github.com/codesoap/pbf-reblob/releases/download/v0.1.0/compare.png)

# Installation
You can download the tool from the releases page:
https://github.com/codesoap/pbf-reblob/releases

If you have the Go toolchain installed and prefer to build pbf-reblob
yourself, you can get it by running this:

```bash
go install github.com/codesoap/pbf-reblob@latest
# The binary is now at ~/go/bin/pbf-reblob.
```

# Usage
```console
$ # Change blob size to roughly 16MiB (while not surpassing this blob size):
$ pbf-reblob serbia-latest.osm.pbf serbia-latest-16M.osm.pbf

$ # Change blob size to roughly 32MiB, with zstd compression:
$ pbf-reblob -s 32M -c zstd serbia-latest.osm.pbf serbia-latest-32M.zstd.osm.pbf

$ # Let's see the results:
$ du -h serbia-latest*
190M    serbia-latest-16M.osm.pbf
186M    serbia-latest-32M.zstd.osm.pbf
194M    serbia-latest.osm.pbf

$ pbf-reblob -h
Usage:
  pbf-reblob [-v] [-s <size>] [-c <compression>] <IN_FILE> <OUT_FILE>
Options:
  -c string
        output compression; either 'raw', 'zlib' or 'zstd' (default "zlib")
  -s string
        uncompressed blob size limit; suffixes 'k' and 'M' allowed (default "16M")
  -v    verbose
```

# How It Works
PBF files contain numerous blobs of OSM entities. The popular tool
[osmium](https://osmcode.org/osmium-tool/) usually puts one group of
~8000 OSM entities into each blob. However the file format allows for
uncompressed blob sizes of up to 32MiB, while recommending sizes up to
16MiB. These sizes are not reached with ~8000 OSM entities.

By moving multiple entity groups into a single blob, the amount of blobs
in a PBF file can be significantly reduced. This reduces file size, because
each blob contains a list of used strings for the entities in the blob. If
there are multiple small blobs, the same strings will often be stored
multiple times (once for each block that uses it). By reducing the
amount of blobs, the amount of duplicate strings can be reduced.

This seems to be most effective with small PBF files. I assume this is
because within a smaller area, there is a higher chance for the same
strings to be reused.

# Side Effects
While no data is lost with this method of compression, the changed blob
size might affect the tools working with PBF files. Most prominently,
many tools will likely use more memory when working with larger blobs.
