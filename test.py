import os
import tkinter as tk
from tkinter import filedialog, messagebox
import csv

def split_large_csv_files(src_folder, size_limit=48):
    """
    Processes CSV files in the given folder. If a file is larger than the size limit (in MB),
    it will be split into smaller files. Each split file will maintain the header row and ensure 
    data integrity without truncating rows.
    :param src_folder: The source folder path where the CSV files are located.
    :param size_limit: The size limit for CSV files in MB. Files larger than this limit will be split.
    """
    for root, dirs, files in os.walk(src_folder):
        for file in files:
            if file.lower().endswith('.csv'):  # Only process CSV files
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert size to MB

                # Process file if it's larger than the specified limit
                if file_size >= size_limit:
                    print(f"Processing file {file} of size {file_size:.2f} MB")

                    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        header = next(reader)  # Read the header row
                        rows = list(reader)  # Read all the data rows

                    # Start splitting the file into smaller files
                    data_chunk = []
                    current_size = 0
                    part_number = 1

                    for row in rows:
                        # Add row to current chunk
                        data_chunk.append(row)
                        current_size += len(','.join(row))  # Approximate size by row length

                        # If current size exceeds the size limit, write to a new file
                        if current_size >= size_limit * 1024 * 1024:
                            # Define the new split file path
                            new_file_path = os.path.join(root, f"{file}_part_{part_number}.csv")
                            with open(new_file_path, mode='w', newline='', encoding='utf-8') as new_file:
                                writer = csv.writer(new_file)
                                writer.writerow(header)  # Write header to the new file
                                writer.writerows(data_chunk)  # Write the data chunk to the new file

                            print(f"Created: {new_file_path}")

                            # Reset for the next chunk
                            part_number += 1
                            data_chunk = []
                            current_size = 0

                    # If there are any remaining rows, write them to a final file
                    if data_chunk:
                        new_file_path = os.path.join(root, f"{file}_part_{part_number}.csv")
                        with open(new_file_path, mode='w', newline='', encoding='utf-8') as new_file:
                            writer = csv.writer(new_file)
                            writer.writerow(header)  # Write header to the new file
                            writer.writerows(data_chunk)  # Write the remaining data
                        print(f"Created: {new_file_path}")
                    
                    # Delete the large file
                    # os.remove(file_path)
                    # print(f"Deleted: {file} ({file_size:.2f} MB)")
                else:
                    print(f"File {file} is under {size_limit} MB, keeping it as is.")

def open_folder_dialog():
    """ Open a folder dialog for selecting the folder """
    folder_path = filedialog.askdirectory()
    if folder_path:  # If a folder was selected
        split_large_csv_files(folder_path)
        messagebox.showinfo("Success", "Files processed successfully!")

def create_gui():
    """ Create the GUI for selecting the folder """
    root = tk.Tk()
    root.title("Large CSV File Splitter")

    # Set the window size
    root.geometry("400x200")

    # Create a label
    label = tk.Label(root, text="Select a folder containing CSV files", font=("Arial", 12))
    label.pack(pady=20)

    # Create a button to open the folder dialog
    button = tk.Button(root, text="Browse Folder", command=open_folder_dialog, font=("Arial", 12))
    button.pack(pady=10)

    # Start the GUI
    root.mainloop()

# Run the GUI
create_gui()
