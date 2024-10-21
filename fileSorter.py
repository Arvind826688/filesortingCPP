import os
import hashlib
import shutil
import threading
import queue
from tkinter import Tk, filedialog, Button, Label, StringVar, messagebox
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Global variables
log_mutex = threading.Lock()
queue_mutex = threading.Lock()
log_file = None
recovery_file = "recovery.txt"
allowed_types = ["jpg", "png", "txt", "pdf", "docx"]  # Modify the list to filter specific file types
processed_files = set()
file_queue = queue.Queue()

# Logging function
def log_operation(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_mutex:
        log_file.write(f"[{timestamp}] [{level}] {message}\n")
        log_file.flush()

# MD5 checksum function for deduplication
def calculate_checksum(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hasher.update(byte_block)
    return hasher.hexdigest()

# Load recovery file (processed files)
def load_recovery():
    try:
        with open(recovery_file, "r") as f:
            return set(line.strip() for line in f)
    except FileNotFoundError:
        return set()

# Save processed file path to recovery file
def save_to_recovery(file_path):
    with open(recovery_file, "a") as f:
        f.write(file_path + "\n")

# Worker thread function to process files
def worker(root_dir, file_hashes, duplicate_dir):
    while not file_queue.empty():
        with queue_mutex:
            file_path = file_queue.get()

        try:
            # Skip files already processed
            if file_path in processed_files:
                continue

            # Sort file based on extension
            extension = file_path.suffix.lstrip(".").lower() or "no_extension"
            dest_dir = os.path.join(root_dir, extension)
            os.makedirs(dest_dir, exist_ok=True)

            # Deduplication using checksum
            file_hash = calculate_checksum(file_path)
            if file_hash in file_hashes:
                # Move duplicate file to the "duplicate_files" folder
                duplicate_dest = os.path.join(duplicate_dir, file_path.name)
                os.makedirs(duplicate_dir, exist_ok=True)
                shutil.move(file_path, duplicate_dest)
                log_operation(f"Duplicate file found and moved: {file_path} -> {duplicate_dest}")
            else:
                dest_path = os.path.join(dest_dir, file_path.name)
                shutil.move(file_path, dest_path)
                log_operation(f"Moved: {file_path} -> {dest_path}")
                file_hashes[file_hash] = dest_path

            # Update recovery and processed files
            processed_files.add(file_path)
            save_to_recovery(str(file_path))
        except Exception as e:
            log_operation(f"Error moving file {file_path}: {e}", "ERROR")

# Filter files based on allowed types
def filter_files(file_path):
    extension = file_path.suffix.lstrip(".").lower()
    return extension in allowed_types

# Function to sort files using multi-threading
def sort_files(root_dir, thread_count):
    global processed_files
    processed_files = load_recovery()

    # Create the "duplicate_files" directory in the root folder
    duplicate_dir = os.path.join(root_dir, "duplicate_files")

    # Gather all files recursively
    all_files = [Path(file) for file in Path(root_dir).rglob('*') if file.is_file() and file not in processed_files]
    for file_path in all_files:
        if filter_files(file_path):
            file_queue.put(file_path)

    total_files = file_queue.qsize()
    if total_files == 0:
        messagebox.showerror("No Files", "No files to process in the selected folder.")
        return

    # Notify user that sorting has started
    print("Sorting...")

    # Create thread pool and start worker threads
    file_hashes = {}
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        for _ in range(thread_count):
            executor.submit(worker, root_dir, file_hashes, duplicate_dir)

    log_operation("File sorting completed.")
    delete_empty_folders(root_dir)

    # Notify user that sorting has completed
    print("Completed.")

# Function to delete empty folders after sorting
def delete_empty_folders(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        try:
            # Ensure the directory is empty before attempting deletion
            if not dirnames and not filenames:
                shutil.rmtree(dirpath, ignore_errors=True)
                log_operation(f"Deleted empty folder: {dirpath}")
        except Exception as e:
            log_operation(f"Failed to delete {dirpath}: {e}", "ERROR")

# GUI to select folder and start sorting
def start_sorting():
    def on_start():
        selected_folder = folder_var.get()
        if not selected_folder:
            messagebox.showerror("Error", "Please select a folder to sort.")
            return

        try:
            thread_count = os.cpu_count() or 4  # Default to 4 if detection fails
            threading.Thread(target=sort_files, args=(selected_folder, thread_count)).start()

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {e}")
            log_operation(f"An error occurred: {e}", "ERROR")

    # GUI setup
    global log_file
    log_file = open("log.txt", "a")

    root = Tk()
    root.title("File Sorter")

    folder_var = StringVar()

    def browse_folder():
        folder_selected = filedialog.askdirectory()
        folder_var.set(folder_selected)

    Label(root, text="Select a folder to sort:").pack(pady=10)
    Button(root, text="Browse", command=browse_folder).pack(pady=5)

    Label(root, textvariable=folder_var).pack(pady=5)

    Button(root, text="Start Sorting", command=on_start).pack(pady=20)

    root.mainloop()

    log_file.close()

# Run the GUI application
if __name__ == "__main__": 
    start_sorting()
