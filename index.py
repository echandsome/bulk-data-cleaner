import os
import pandas as pd
import zipfile
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
from tkinter import *
from tkinter import filedialog, messagebox, ttk
import multiprocessing
import tempfile
import shutil
from datetime import datetime

def excel_col_to_index(col):
    index = 0
    for i, char in enumerate(reversed(col)):
        index += (ord(char.upper()) - 64) * (26 ** i)
    return index - 1

EXCEL_INDEX = {
    'country': excel_col_to_index('W'),
    'language': excel_col_to_index('BE'),
    'occupation': excel_col_to_index('K'),
    'industry': excel_col_to_index('BG')
}

def process_group_external(country, records, output_dir):
    df = pd.DataFrame(records)
    sorted_df = df.sort_values(by=['Language', 'Occupation', 'Industry'])
    filename = os.path.join(output_dir, f"{country}.csv")
    sorted_df.to_csv(filename, index=False)

class ExcelProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel Bulk Processor (Column Index Based)")

        self.queue = []
        self.processing = False
        self.archive = []
        self.save_dir = None

        self.setup_gui()

    def setup_gui(self):
        frm = Frame(self.root)
        frm.pack(padx=10, pady=10)

        self.file_label = Label(frm, text="No file selected")
        self.file_label.grid(row=0, column=0, columnspan=2)

        Button(frm, text="Select File", command=self.select_file).grid(row=1, column=0, columnspan=2, sticky='ew')

        Button(frm, text="Select Save Folder", command=self.select_save_folder).grid(row=2, column=0, columnspan=2, sticky='ew', pady=(10, 0))

        self.queue_box = Listbox(frm, height=8, width=50)
        self.queue_box.grid(row=3, column=0, columnspan=2, pady=5)

        Button(frm, text="Start Processing", command=self.start_processing).grid(row=4, column=0, sticky='ew')
        Button(frm, text="Pause", command=self.pause_processing).grid(row=4, column=1, sticky='ew')

        self.status_label = Label(frm, text="Waiting...")
        self.status_label.grid(row=5, column=0, columnspan=2)

        self.progress = ttk.Progressbar(frm, orient=HORIZONTAL, length=400, mode='determinate')
        self.progress.grid(row=6, column=0, columnspan=2, pady=5)

    def select_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            self.queue.append(file_path)
            self.queue_box.insert(END, os.path.basename(file_path))
            self.file_label.config(text="No file selected")

    def select_save_folder(self):
        folder_path = filedialog.askdirectory()
        if folder_path:
            self.save_dir = folder_path
            self.status_label.config(text=f"Save folder selected: {folder_path}")

    def pause_processing(self):
        self.processing = False
        self.status_label.config(text="Paused")

    def start_processing(self):
        if self.processing:
            return
        self.processing = True
        threading.Thread(target=self.process_queue).start()

    def process_queue(self):
        while self.queue and self.processing:
            file = self.queue.pop(0)
            self.queue_box.delete(0)
            self.status_label.config(text=f"Processing {os.path.basename(file)}...")
            self.process_file(file)
            self.archive.append(file)
            self.status_label.config(text=f"{os.path.basename(file)} completed")
        self.status_label.config(text="All tasks completed")

    def process_file(self, file_path):
        temp_dir = tempfile.mkdtemp()

        df = pd.read_excel(file_path, header=None, engine='openpyxl')
        df.columns = [f"Column{i+1}" for i in range(df.shape[1])]

        try:
            rename_map = {
                df.columns[EXCEL_INDEX['country']]: 'Country',
                df.columns[EXCEL_INDEX['language']]: 'Language',
                df.columns[EXCEL_INDEX['occupation']]: 'Occupation',
                df.columns[EXCEL_INDEX['industry']]: 'Industry'
            }
            df = df.rename(columns=rename_map)
        except IndexError:
            self.status_label.config(text="Specified column index exceeds range")
            return

        df = self.clean_data(df)

        new_columns = []
        for i, col in enumerate(df.columns):
            if col in ['Country', 'Language', 'Occupation', 'Industry']:
                new_columns.append(col)
            else:
                new_columns.append(f"Column{i+1}")
        df.columns = new_columns

        grouped = df.groupby('Country')
        output_dir = os.path.join(os.path.dirname(temp_dir), "processed")
        os.makedirs(output_dir, exist_ok=True)

        num_workers = max(1, int(multiprocessing.cpu_count() * 0.5))
        futures = []

        try:
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                for country, group in grouped:
                    records = group.to_dict('records')
                    futures.append(
                        executor.submit(process_group_external, country, records, output_dir)
                    )

                total = len(futures)
                for i, f in enumerate(as_completed(futures), start=1):
                    self.progress["value"] = int((i / total) * 100)
                    self.root.update_idletasks()

            self.zip_output(output_dir, os.path.dirname(file_path))
        except Exception as e:
            self.status_label.config(text=f"Error during sorting: {str(e)}")

        try:
            filtered_dir = os.path.join(os.path.dirname(temp_dir), "rachInbox")
            os.makedirs(filtered_dir, exist_ok=True)

            for file in os.listdir(output_dir):
                if file.endswith(".csv"):
                    _file_path = os.path.join(output_dir, file)
                    df = pd.read_csv(_file_path)
                    df.columns = [f"Column{i+1}" for i in range(df.shape[1])]
                    try:
                        filtered_df = pd.DataFrame({
                            "Email": df.iloc[:, 0],
                            "First_Name": df.iloc[:, 2],
                            "Last_Name": df.iloc[:, 3],
                            "Company_Name": df.iloc[:, 13],
                            "Linkdin": df.iloc[:, 12],
                            "Personalised_Lines": df.iloc[:, 31]
                        })
                        filtered_filename = os.path.splitext(file)[0] + "_rachInbox.csv"
                        filtered_df.to_csv(os.path.join(filtered_dir, filtered_filename), index=False)
                    except IndexError:
                        print(f"Skipping {file}: One or more required columns are missing.")
            self.zip_output(filtered_dir, os.path.dirname(file_path))
        except Exception as e:
            self.status_label.config(text=f"Error during filtering: {str(e)}")

        try:
            filtered_dir = os.path.join(os.path.dirname(temp_dir), "ghl")
            os.makedirs(filtered_dir, exist_ok=True)

            for file in os.listdir(output_dir):
                if file.endswith(".csv"):
                    _file_path = os.path.join(output_dir, file)
                    df = pd.read_csv(_file_path)
                    df.columns = [f"Column{i+1}" for i in range(df.shape[1])]
                    try:
                        filtered_df = pd.DataFrame({
                            "Email": df.iloc[:, 0],
                            "First_Name": df.iloc[:, 1],
                            "Last_Name": df.iloc[:, 2],
                            "Department": df.iloc[:, 5],
                            "Job_Title": df.iloc[:, 6],
                            "Job_Level": df.iloc[:, 7],
                            "City": df.iloc[:, 8],
                            "State": df.iloc[:, 9],
                            "Country": df.iloc[:, 10],
                            "LinkedIn_Profile": df.iloc[:, 12],
                            "Employer": df.iloc[:, 13],
                            "Employer_Website": df.iloc[:, 14],
                            "Phone": df.iloc[:, 15],
                            "Employer_Facebook": df.iloc[:, 16],
                            "Employer_LinkedIn": df.iloc[:, 17],
                            "Employer_Founded_Date": df.iloc[:, 21],
                            "Employer_Zip": df.iloc[:, 24],
                            "Languages_Spoken": df.iloc[:, 27],
                            "Industry": df.iloc[:, 28],
                            "Focus": df.iloc[:, 29],
                            "Skills": df.iloc[:, 30]
                        })
                        filtered_filename = os.path.splitext(file)[0] + "_ghl.csv"
                        filtered_df.to_csv(os.path.join(filtered_dir, filtered_filename), index=False)
                    except IndexError:
                        print(f"Skipping {file}: One or more required columns are missing.")
            self.zip_output(filtered_dir, os.path.dirname(file_path))
        except Exception as e:
            self.status_label.config(text=f"Error during filtering: {str(e)}")

        shutil.rmtree(temp_dir)

    def clean_data(self, df):
        data_only = df.iloc[1:, :]
        valid_cols = ~data_only.apply(lambda col: col.isna().all() or col.astype(str).str.strip().eq('').all())
        special_cols = ~data_only.apply(lambda col: col.astype(str).str.contains(r"#!\$@\-").any())
        df = df.loc[:, valid_cols & special_cols]
        return df

    def zip_output(self, source_dir, default_destination_dir):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_name = os.path.basename(source_dir.rstrip(os.sep))
        zip_filename = f"{folder_name}_{timestamp}.zip"
        zip_path = os.path.join(os.path.dirname(source_dir), zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in os.listdir(source_dir):
                full_path = os.path.join(source_dir, file)
                if os.path.isfile(full_path):
                    zipf.write(full_path, arcname=file)

        destination_dir = self.save_dir if self.save_dir else default_destination_dir
        os.makedirs(destination_dir, exist_ok=True)

        final_zip_path = os.path.join(destination_dir, zip_filename)
        shutil.move(zip_path, final_zip_path)

        # self.status_label.config(text=f"Compression completed: {final_zip_path}")

if __name__ == "__main__":
    root = Tk()
    app = ExcelProcessorApp(root)
    root.mainloop()
