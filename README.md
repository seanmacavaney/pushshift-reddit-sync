# pushshift-reddit-sync

A simple utility to sync a local copy of the [pushshift.io Reddit submission and comment dumps](https://files.pushshift.io/reddit/).
The code verifies the download's hash matches the expected value, and optionally normalizes the compression to lz4 for faster decompression.

Example:

```
python main.py --comments --submissions --out_dir ~/path/to/archive --compression lz4
```
