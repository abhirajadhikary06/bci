import os

from dotenv import load_dotenv

from zipzap.main import authenticate_dropbox, batch_compress_dropbox


def main() -> None:
	load_dotenv()

	if not os.getenv("DROPBOX_ACCESS_TOKEN"):
		print("Missing DROPBOX_ACCESS_TOKEN in .env")
		return

	dbx = authenticate_dropbox()
	# This opens an interactive folder picker and uploads compressed files
	# into a 'Compressed_Images' subfolder in the selected Dropbox folder.
	batch_compress_dropbox(dbx)


if __name__ == "__main__":
	main()
