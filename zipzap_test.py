import os
import sys

from zipzap.main import batch_compress_local_zip


def main() -> None:
	# Accept ZIP path from CLI arg first, then fall back to prompt.
	zip_path = sys.argv[1] if len(sys.argv) > 1 else input("Enter ZIP path: ").strip().strip('"')

	if not zip_path:
		print("No ZIP path provided.")
		return

	if not os.path.isfile(zip_path):
		print(f"ZIP file not found: {zip_path}")
		return

	batch_compress_local_zip(zip_path)


if __name__ == "__main__":
	main()
