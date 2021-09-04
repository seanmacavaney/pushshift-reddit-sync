import requests
import argparse
from pathlib import Path
import ir_datasets
import io
import bz2
import lzma
import lz4.frame
import zstandard
from contextlib import ExitStack
from hashlib import sha256

logger = ir_datasets.log.easy()

def get_hashes(url):
  t = requests.get(url).text
  result = {}
  for line in t.split('\n'):
    if line.strip():
      hsh, file = line.split()
      result[file] = hsh
  return result

def fetch_files(files, base_url, out_dir, compression):
  if not out_dir.exists():
    out_dir.mkdir(parents=True)
  for file, expected_sha256 in files.items():
    local_file = out_dir/file
    if compression == 'lz4':
      local_file = local_file.with_suffix('.lz4')
    if local_file.exists():
      logger.info(f'{local_file} exists')
      continue
    with ExitStack() as stack:
      fout = stack.enter_context(ir_datasets.util.finialized_file(str(local_file), 'wb'))
      if compression == 'lz4':
        fout = stack.enter_context(lz4.frame.LZ4FrameFile(fout, 'wb'))
      resp = stack.enter_context(requests.get(base_url + file, stream=True))
      fin = io.BufferedReader(ir_datasets.util.IterStream(iter_pbar(file, resp, expected_sha256)))
      if compression != 'default':
        if file.endswith('.bz2'):
          fin = bz2.BZ2File(fin)
        if file.endswith('.xz'):
          fin = lzma.LZMAFile(fin)
        if file.endswith('.zst'):
          dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
          fin = dctx.stream_reader(fin)
      while True:
        chunk = fin.read(io.DEFAULT_BUFFER_SIZE)
        if not chunk:
          break
        fout.write(chunk)

def iter_pbar(file, resp, expected_sha256):
  size = resp.headers.get('content-length')
  if size:
    size = int(size)
  hasher = sha256()
  with logger.pbar_raw(desc=file, unit='B', unit_scale=True, unit_divisor=1024, total=size) as pbar:
    for chunk in resp.iter_content(chunk_size=io.DEFAULT_BUFFER_SIZE):
      pbar.update(len(chunk))
      hasher.update(chunk)
      yield chunk
  if hasher.hexdigest() != expected_sha256:
    raise RuntimeError(f'hash mismatch {hasher.hexdigest()} != {expected_sha256}')

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--comments', action='store_true')
  parser.add_argument('--submissions', action='store_true')
  parser.add_argument('--compression', default='default')
  parser.add_argument('--out_dir', default=Path.home()/'data'/'reddit', type=Path)
  args = parser.parse_args()
  if args.comments:
    comment_files = get_hashes('https://files.pushshift.io/reddit/comments/sha256sum.txt')
    fetch_files(comment_files, 'https://files.pushshift.io/reddit/comments/', args.out_dir/'comments', args.compression)
  if args.submissions:
    comment_files = get_hashes('https://files.pushshift.io/reddit/submissions/sha256sums.txt')
    fetch_files(comment_files, 'https://files.pushshift.io/reddit/submissions/', args.out_dir/'submissions', args.compression)
  if not args.comments and not args.submissions:
    logger.info('should specify --comments and/or --submissions')

if __name__ == '__main__':
  main()
