import os
import hashlib
import shutil
from pathlib import Path
import concurrent.futures
import threading
import queue

# Mutex for synchronized access to shared resources (log file, file list)
log_mutex = threading.Lock()
queue_mutex = threading.Lock()
log_file = open("log.txt", "a")

# Thread-safe logging function
def log_operation(message):
    with log_mutex:
        log_file.write(message + "\n")
        log_file.flush()

# Function to calculate MD5 hash of a file
def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

# Function to move a file to the appropriate directory based on extension
def move_file(src, dest_dir, file_hashes):
    try:
        # Check if destination directory exists, if not create it
        os.makedirs(dest_dir, exist_ok=True)

        # Check file content with MD5 checksum
        file_hash = calculate_md5(src)
        if file_hash in file_hashes:
            new_name = f"{src.stem}_duplicate{src.suffix}"
            duplicate_dest = dest_dir / new_name
            shutil.move(src, duplicate_dest)
            log_operation(f"Duplicate file found: {src} (renamed to: {duplicate_dest})")
        else:
            dest_path = dest_dir / src.name
            shutil.move(src, dest_path)
            log_operation(f"Moved: {src} -> {dest_path}")
            file_hashes[file_hash] = str(dest_path)
    except Exception as e:
        log_operation(f"Error moving file: {e}")

# Function to load recovery state
def load_recovery(recovery_file):
    processed_files = set()
    if os.path.exists(recovery_file):
        with open(recovery_file, "r") as f:
            for line in f:
                processed_files.add(line.strip())
    return processed_files

# Function to save sorted files to recovery file
def save_to_recovery(recovery_file, file_path):
    with open(recovery_file, "a") as f:
        f.write(file_path + "\n")

# Function to print progress bar
def print_progress(current, total):
    bar_width = 70
    progress = current / total
    pos = int(bar_width * progress)
    bar = "=" * pos + ">" + " " * (bar_width - pos)
    print(f"[{bar}] {int(progress * 100)} %", end="\r")

# Thread worker function
def worker(files_queue, root_dir, processed_files, file_hashes, total_files):
    processed_files_count = 0
    while True:
        with queue_mutex:
            if files_queue.empty():
                break
            file_path = files_queue.get()

        # Get the file extension and create a folder for it
        extension = file_path.suffix or "no_extension"
        dest_dir = root_dir / extension

        # Check for duplicate processing of files
        if str(file_path) not in processed_files:
            move_file(file_path, dest_dir, file_hashes)
            processed_files.add(str(file_path))
            save_to_recovery("recovery.txt", str(file_path))

        processed_files_count += 1
        print_progress(processed_files_count, total_files)

# Function to delete empty folders after sorting
def delete_empty_folders(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Check if directory is empty
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
                log_operation(f"Deleted empty folder: {dirpath}")
            except OSError as e:
                log_operation(f"Error deleting folder: {e}")

# Function to sort files using multithreading
def sort_files(root_dir, thread_count):
    if not root_dir.exists() or not root_dir.is_dir():
        print("Invalid directory!")
        return

    processed_files = load_recovery("recovery.txt")
    file_hashes = {}
    files_queue = queue.Queue()

    # Recursively collect all files
    for file_path in root_dir.rglob('*'):
        if file_path.is_file() and str(file_path) not in processed_files:
            files_queue.put(file_path)

    total_files = files_queue.qsize()
    if total_files == 0:
        print("No files to process!")
        return

    # Create thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(worker, files_queue, root_dir, processed_files, file_hashes, total_files)
                   for _ in range(thread_count)]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("\nFile sorting completed successfully!")

    # Delete empty folders
    delete_empty_folders(root_dir)

if __name__ == "__main__":
    root_folder = input("Enter the root folder to sort files: ")
    root_dir = Path(root_folder)

    try:
        log_operation("Starting file sorting...")
        thread_count = os.cpu_count()  # Use the maximum available threads
        sort_files(root_dir, thread_count)
        log_operation("File sorting completed successfully.")
    except Exception as e:
        log_operation(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

    log_file.close()
