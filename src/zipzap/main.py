import os
import io
import time
import threading
import queue
import zipfile
import tempfile
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# Dropbox dependency
try:
    import dropbox
except ImportError:
    print("Install Dropbox dependencies: pip install dropbox")
    exit(1)

# pyvips dependency (now used for all formats)
try:
    import pyvips
except ImportError:
    print("Install pyvips dependency: pip install pyvips")
    exit(1)

# Compression settings — tuned more aggressively for speed
CPU_COUNT = os.cpu_count() or 4

# Reduced number of download workers — high values often cause worse performance
DOWNLOAD_WORKERS = CPU_COUNT * 2
COMPRESS_WORKERS = CPU_COUNT
UPLOAD_WORKERS  = CPU_COUNT * 2

DEST_FOLDER_NAME = "Compressed_Images"
JPEG_QUALITY = int(os.getenv("BCI_JPEG_QUALITY", "75"))   # slightly lower default
JPEG_QUALITY = max(1, min(92, JPEG_QUALITY))

VERBOSE_FILE_LOGS = os.getenv("BCI_VERBOSE_FILE_LOGS", "0") == "1"

LARGE_UPLOAD_THRESHOLD = 8 * 1024 * 1024
QUEUE_MAXSIZE = 60

IMAGE_EXTS = {
    '.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tif', '.tiff',
    '.gif', '.avif', '.heic', '.heif', '.jfif', '.ico'
}

download_queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=QUEUE_MAXSIZE)
upload_queue:   "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=QUEUE_MAXSIZE)

WORKER_JPEG_QUALITY = JPEG_QUALITY


def configure_vips_concurrency(target: int) -> bool:
    try:
        if hasattr(pyvips, "concurrency_set"):
            pyvips.concurrency_set(target)
            return True
        lib = getattr(pyvips, "vips_lib", None)
        if lib and hasattr(lib, "vips_concurrency_set"):
            lib.vips_concurrency_set(target)
            return True
    except Exception:
        pass
    return False


def is_likely_image(filename: str) -> bool:
    return any(filename.lower().endswith(ext) for ext in IMAGE_EXTS)


def _drain_queue(q: queue.Queue):
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            break


def authenticate_dropbox() -> dropbox.Dropbox:
    access_token = os.getenv('DROPBOX_ACCESS_TOKEN')
    if not access_token:
        print("Dropbox access token is required.")
        exit(1)

    # Strict token-only auth path (no refresh-token/app-key flow).
    dbx = dropbox.Dropbox(oauth2_access_token=access_token)
    try:
        dbx.users_get_current_account()
    except dropbox.exceptions.AuthError:
        print("Invalid or expired Dropbox access token.")
        exit(1)
    except Exception as e:
        print(f"Dropbox authentication failed: {e}")
        exit(1)
    return dbx


@dataclass
class PipelineStats:
    total_files: int = 0
    compressed_files: int = 0
    skipped_files: int = 0
    uploaded_files: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    compression_time_seconds: float = 0.0


def init_compression_worker(jpeg_quality: int):
    global WORKER_JPEG_QUALITY
    WORKER_JPEG_QUALITY = jpeg_quality
    configure_vips_concurrency(CPU_COUNT)


# ──────────────────────────────────────────────────────────────────────────────
#   Dropbox listing / download / upload functions (unchanged)
# ──────────────────────────────────────────────────────────────────────────────

def list_dropbox_root_folders(dbx: dropbox.Dropbox) -> List[dropbox.files.FolderMetadata]:
    try:
        result = dbx.files_list_folder("")
        folders = [e for e in result.entries if isinstance(e, dropbox.files.FolderMetadata)]
        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            folders.extend([e for e in result.entries if isinstance(e, dropbox.files.FolderMetadata)])
        return folders
    except Exception as e:
        print(f"Error listing root folders: {e}")
        return []


def list_dropbox_files(dbx: dropbox.Dropbox, folder_path: str) -> List[dropbox.files.FileMetadata]:
    try:
        result = dbx.files_list_folder(folder_path)
        files = [e for e in result.entries if isinstance(e, dropbox.files.FileMetadata) and is_likely_image(e.name)]
        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            files.extend([e for e in result.entries if isinstance(e, dropbox.files.FileMetadata) and is_likely_image(e.name)])
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []


def download_dropbox_file(dbx: dropbox.Dropbox, file_path: str) -> bytes:
    _, response = dbx.files_download(file_path)
    return response.content


def upload_dropbox_file(dbx: dropbox.Dropbox, file_data: bytes, filename: str, folder_path: str):
    target_path = f"{folder_path}/{filename}"

    if len(file_data) <= LARGE_UPLOAD_THRESHOLD:
        dbx.files_upload(file_data, target_path, mode=dropbox.files.WriteMode('overwrite'))
        return

    # Session upload for large files
    chunk_size = LARGE_UPLOAD_THRESHOLD
    session = dbx.files_upload_session_start(file_data[:chunk_size])
    cursor = dropbox.files.UploadSessionCursor(session_id=session.session_id, offset=chunk_size)
    commit = dropbox.files.CommitInfo(path=target_path, mode=dropbox.files.WriteMode('overwrite'))

    offset = chunk_size
    while offset < len(file_data):
        chunk = file_data[offset:offset + chunk_size]
        if len(chunk) < chunk_size:
            dbx.files_upload_session_finish(chunk, cursor, commit)
            break
        dbx.files_upload_session_append_v2(chunk, cursor)
        offset += chunk_size
        cursor.offset = offset


def create_dropbox_folder(dbx: dropbox.Dropbox, folder_path: str):
    try:
        dbx.files_create_folder_v2(folder_path)
    except dropbox.exceptions.ApiError as e:
        if 'already_exists' not in str(e).lower():
            raise
        # folder already exists — ok


def _guess_mime_type(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower().lstrip('.')
    mapping = {
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'jfif': 'image/jpeg',
        'png': 'image/png',
        'webp': 'image/webp',
        'bmp': 'image/bmp',
        'tif': 'image/tiff', 'tiff': 'image/tiff',
        'gif': 'image/gif',
        'avif': 'image/avif',
        'heic': 'image/heic', 'heif': 'image/heic',
    }
    return mapping.get(ext, 'application/octet-stream')


# ──────────────────────────────────────────────────────────────────────────────
#   Main compression function — now ALL using pyvips + speed-focused settings
# ──────────────────────────────────────────────────────────────────────────────

def compress_image(image_data: bytes, original_filename: str) -> Tuple[bytes, str, str, bool, float]:
    started = time.perf_counter()
    try:
        name_no_ext, ext = os.path.splitext(original_filename)
        ext = ext.lower()

        image = pyvips.Image.new_from_buffer(image_data, "", access="sequential")

        if ext in ('.jpg', '.jpeg', '.jfif'):
            # Remove optimize_coding=true → default false for ~5-10% faster encode
            candidate_data = image.write_to_buffer(f'.jpg[Q={WORKER_JPEG_QUALITY},strip=true]')  # No optimize_coding
            candidate_name = f"{name_no_ext}.jpg"
            candidate_mime = 'image/jpeg'

        elif ext == '.png':
            # compression=1 (was 6) for ~2x faster deflate
            candidate_data = image.write_to_buffer('.png[compression=1,strip=true,palette=false]')
            candidate_name = f"{name_no_ext}.png"
            candidate_mime = 'image/png'

        elif ext == '.webp':
            # effort=0 (was 2) for fastest encode (~20-30% speedup)
            candidate_data = image.write_to_buffer('.webp[Q=75,effort=0,strip=true]')
            candidate_name = f"{name_no_ext}.webp"
            candidate_mime = 'image/webp'

        elif ext in ('.avif', '.heif', '.heic'):
            # effort=0 (was 2) for minimal CPU (~2x faster per benchmarks)
            candidate_data = image.write_to_buffer('.avif[Q=60,effort=0,strip=true]')
            candidate_name = f"{name_no_ext}.avif"
            candidate_mime = 'image/avif'

        elif ext in ('.tif', '.tiff'):
            # lzw usually faster than zstd for most photographic content
            candidate_data = image.write_to_buffer('.tiff[compression=lzw,strip=true,tile=true]')
            candidate_name = f"{name_no_ext}.tif"
            candidate_mime = 'image/tiff'

        else:  # gif, bmp, unknown → png fast
            # compression=1 (was 6)
            candidate_data = image.write_to_buffer('.png[compression=1,strip=true]')
            candidate_name = f"{name_no_ext}.png"
            candidate_mime = 'image/png'

        # Always use re-encoded output, even if it is not smaller than the original.
        return candidate_data, candidate_name, candidate_mime, True, time.perf_counter() - started

    except Exception as e:
        print(f"Compression error {original_filename}: {e}")
        return image_data, original_filename, _guess_mime_type(original_filename), False, time.perf_counter() - started


def compress_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload['data']
    name = payload['name']
    out_data, out_name, _, applied, seconds = compress_image(data, name)

    result = {
        'path_lower': payload.get('path_lower'),
        'original_name': name,
        'original_size': len(data),
        'compressed_applied': applied,
        'compression_seconds': seconds,
        'output_name': out_name if applied else name,
        'output_data': out_data if applied else data,
    }
    # Preserve any extra fields (like rel_path)
    for k, v in payload.items():
        if k not in result and k not in {'data', 'name'}:
            result[k] = v
    return result


# ──────────────────────────────────────────────────────────────────────────────
#   Pipeline – simplified threading model
# ──────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    items: List[Any],
    read_item,
    write_item,
    benchmark: bool = False,
) -> Tuple[PipelineStats, float]:
    started = time.perf_counter()
    _drain_queue(download_queue)
    _drain_queue(upload_queue)

    stats = PipelineStats(total_files=len(items))
    stats_lock = threading.Lock()

    def compression_worker(compress_pool: ProcessPoolExecutor):
        futures = []
        while True:
            payload = download_queue.get()
            if payload is None:
                break
            fut = compress_pool.submit(compress_task, payload)
            futures.append(fut)

        for fut in as_completed(futures):
            result = fut.result()
            with stats_lock:
                stats.input_bytes += result['original_size']
                stats.compression_time_seconds += result['compression_seconds']
                if result['compressed_applied']:
                    stats.compressed_files += 1
                    stats.output_bytes += len(result['output_data'])
                else:
                    stats.skipped_files += 1
                    stats.output_bytes += result['original_size']
            upload_queue.put(result)

        upload_queue.put(None)

    def upload_worker(upload_pool: ThreadPoolExecutor):
        futures = []
        while True:
            payload = upload_queue.get()
            if payload is None:
                break
            futures.append(upload_pool.submit(write_item, payload))

        for fut in as_completed(futures):
            fut.result()  # raise exceptions if any
            with stats_lock:
                stats.uploaded_files += 1

    effective_dl = max(1, min(DOWNLOAD_WORKERS, len(items)))
    effective_cp = max(1, min(COMPRESS_WORKERS, len(items)))
    effective_up = max(1, min(UPLOAD_WORKERS, len(items)))

    if not benchmark and VERBOSE_FILE_LOGS:
        print(f"Workers → dl:{effective_dl}  compress:{effective_cp}  up:{effective_up}")

    with ProcessPoolExecutor(
        max_workers=effective_cp,
        initializer=init_compression_worker,
        initargs=(JPEG_QUALITY,),
    ) as cp_pool, ThreadPoolExecutor(max_workers=effective_up) as up_pool:

        compress_th = threading.Thread(target=compression_worker, args=(cp_pool,), daemon=True)
        upload_th   = threading.Thread(target=upload_worker,   args=(up_pool,),   daemon=True)

        compress_th.start()
        upload_th.start()

        with ThreadPoolExecutor(max_workers=effective_dl) as dl_pool:
            futures = [dl_pool.submit(lambda x: download_queue.put(read_item(x)), item) for item in items]
            wait(futures)

        download_queue.put(None)
        compress_th.join()
        upload_th.join()

    elapsed = time.perf_counter() - started
    return stats, elapsed


def print_summary(stats: PipelineStats, elapsed: float):
    mb_in  = stats.input_bytes / (1024 * 1024)
    mb_out = stats.output_bytes / (1024 * 1024)
    saved  = stats.input_bytes - stats.output_bytes
    pct    = (saved / stats.input_bytes * 100) if stats.input_bytes > 0 else 0
    tp     = mb_in / elapsed if elapsed > 0 else 0

    print("\n=== Summary ===")
    print(f"Files: {stats.total_files}  (compressed: {stats.compressed_files}, skipped: {stats.skipped_files})")
    print(f"Input:  {mb_in:.2f} MB → Output: {mb_out:.2f} MB")
    print(f"Saved:  {saved/(1024*1024):.2f} MB  ({pct:.1f}%)")
    print(f"Compression CPU-seconds (aggregate): {stats.compression_time_seconds:.2f} s")
    print(f"Pipeline wall time:                 {elapsed:.2f} s")
    print(f"Throughput:       {tp:.2f} MB/s")


# ──────────────────────────────────────────────────────────────────────────────
#   Interactive folder selection (unchanged)
# ──────────────────────────────────────────────────────────────────────────────

def select_folder_interactive(items: List[Any], item_type: str) -> Optional[Any]:
    if not items:
        print(f"No {item_type} found.")
        return None

    print(f"\nAvailable {item_type}:")
    for i, item in enumerate(items, 1):
        if item_type == "folders":
            print(f"{i}. {item.name}  ({item.path_lower})")
        else:
            size = getattr(item, 'size', '—')
            print(f"{i}. {item.name}  ({size} bytes)")

    while True:
        try:
            choice = int(input(f"\nSelect {item_type} (1–{len(items)}) or 0=cancel: "))
            if choice == 0:
                return None
            if 1 <= choice <= len(items):
                return items[choice - 1]
            print("Invalid choice.")
        except ValueError:
            print("Enter a number.")


# ──────────────────────────────────────────────────────────────────────────────
#   Dropbox batch
# ──────────────────────────────────────────────────────────────────────────────

def batch_compress_dropbox(dbx: dropbox.Dropbox):
    configure_vips_concurrency(CPU_COUNT)

    folders = list_dropbox_root_folders(dbx)
    folder = select_folder_interactive(folders, "folders")
    if not folder:
        return

    path = folder.path_lower
    print(f"\nSelected: {folder.name}")

    files = list_dropbox_files(dbx, path)
    if not files:
        print("No images found.")
        return

    print(f"Found {len(files)} images.")

    dest_path = f"{path}/{DEST_FOLDER_NAME}"
    create_dropbox_folder(dbx, dest_path)

    def read_item(fm):
        data = download_dropbox_file(dbx, fm.path_lower)
        return {'path_lower': fm.path_lower, 'name': fm.name, 'data': data}

    def write_item(res):
        upload_dropbox_file(dbx, res['output_data'], res['output_name'], dest_path)
        if VERBOSE_FILE_LOGS:
            print(f"Uploaded {res['original_name']}  ({res['original_size']:,} → {len(res['output_data']):,} bytes)")

    stats, elapsed = run_pipeline(files, read_item, write_item)
    print_summary(stats, elapsed)


# ──────────────────────────────────────────────────────────────────────────────
#   Local folder & ZIP
# ──────────────────────────────────────────────────────────────────────────────

def list_local_images(folder_path: str) -> List[str]:
    images = []
    for root, _, files in os.walk(folder_path):
        for f in files:
            if is_likely_image(f):
                images.append(os.path.join(root, f))
    return images


def batch_compress_local_folder(folder_path: str, output_root: Optional[str] = None, benchmark: bool = False):
    if output_root is None:
        output_root = os.path.join(folder_path, DEST_FOLDER_NAME)

    os.makedirs(output_root, exist_ok=True)

    files = list_local_images(folder_path)
    # exclude output folder itself
    files = [p for p in files if os.path.commonpath([p, output_root]) != os.path.abspath(output_root)]

    if not files:
        print("No images found.")
        return PipelineStats(), 0.001

    if not benchmark and VERBOSE_FILE_LOGS:
        print(f"Found {len(files)} images → output: {output_root}")

    def read_item(path: str):
        rel = os.path.relpath(path, folder_path)
        with open(path, 'rb') as f:
            data = f.read()
        return {
            'path_lower': rel,
            'name': os.path.basename(path),
            'data': data,
            'rel_path': rel,
        }

    def write_item(res: Dict):
        rel = res.get('rel_path', res['output_name'])
        out_dir = os.path.join(output_root, os.path.dirname(rel))
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(output_root, rel if res['output_name'] == res['original_name'] else os.path.join(os.path.dirname(rel), res['output_name']))
        with open(out_path, 'wb') as f:
            f.write(res['output_data'])
        if not benchmark and VERBOSE_FILE_LOGS:
            print(f"Saved {res['original_name']}  ({res['original_size']:,} → {len(res['output_data']):,} bytes)")

    stats, elapsed = run_pipeline(files, read_item, write_item, benchmark=benchmark)

    if not benchmark:
        print_summary(stats, elapsed)
        print(f"\nDone. Check: {output_root}")

    return stats, elapsed


def batch_compress_local_zip(zip_path: str):
    if not os.path.isfile(zip_path):
        print("ZIP not found.")
        return

    with tempfile.TemporaryDirectory(prefix='bci_in_') as indir, tempfile.TemporaryDirectory(prefix='bci_out_') as outdir:
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if not m.endswith('/') and is_likely_image(m)]
            if not members:
                print("No images in ZIP.")
                return
            zf.extractall(indir, members)

        stats, elapsed = batch_compress_local_folder(indir, output_root=outdir, benchmark=False)
        print_summary(stats, elapsed)

        out_zip = os.path.join(os.path.dirname(zip_path), f"{os.path.splitext(os.path.basename(zip_path))[0]}_compressed.zip")
        with zipfile.ZipFile(out_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
            for root, _, fnames in os.walk(outdir):
                for fn in fnames:
                    full = os.path.join(root, fn)
                    arc = os.path.relpath(full, outdir)
                    zout.write(full, arc)
        print(f"Created compressed ZIP: {out_zip}")


def main():
    print("=== Fast Batch Image Compression ===")
    print(f" JPEG Q = {JPEG_QUALITY}    CPU = {CPU_COUNT}")

    print("\nSource:")
    print("1. Dropbox")
    print("2. Local folder")
    print("3. Local ZIP file")

    choice = input("→ ").strip()

    if choice == '1':
        dbx = authenticate_dropbox()
        batch_compress_dropbox(dbx)
    elif choice == '2':
        path = input("Folder path: ").strip().strip('"')
        if not os.path.isdir(path):
            print("Invalid path.")
            return
        batch_compress_local_folder(path)
    elif choice == '3':
        path = input("ZIP path: ").strip().strip('"')
        batch_compress_local_zip(path)
    else:
        print("Invalid choice.")


if __name__ == "__main__":
    main()