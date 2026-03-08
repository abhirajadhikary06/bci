# zipzap

Fast batch image compression for local folders, ZIP archives, and Dropbox.

## Prerequisite

Add your Dropbox access token to a `.env` file in your project root:

```env
DROPBOX_ACCESS_TOKEN=your_dropbox_access_token_here
```

## Install

```bash
pip install zipzap
```

## CLI

```bash
zipzap
```

`zipzap` supports:
- Dropbox folder compression
- Local folder compression
- Local ZIP compression

## Python Usage

### Local Folder

```python
from zipzap.main import batch_compress_local_folder

# Compress all images in a local folder
batch_compress_local_folder(r"C:\path\to\images")
```

### Local ZIP

```python
from zipzap.main import batch_compress_local_zip

# Compress images inside a ZIP and create <name>_compressed.zip
batch_compress_local_zip(r"C:\path\to\images.zip")
```

### Dropbox

```python
from zipzap.main import authenticate_dropbox, batch_compress_dropbox

# Compress a Dropbox folder (interactive folder picker)
dbx = authenticate_dropbox()
batch_compress_dropbox(dbx)
```
