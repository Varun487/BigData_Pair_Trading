import os

companies_folders = os.listdir("../Storage/Companies")

print("----------  EXTRACTING CSVS  ----------")

print()
print("Number of companies folders:", len(companies_folders))

# Checking if this step was completed earlier
if not os.path.exists("../Storage/Companies_csvs/"):

    # Making a folder of all csvs
    os.mkdir("../Storage/Companies_csvs/")

    # Extracting csvs present in each companies folder into companies_csv folder for ease of use
    for company_folder in companies_folders:
        print(company_folder)
        if os.path.isdir(f"../Storage/Companies/{company_folder}"):
            for csv_file in os.listdir(f"../Storage/Companies/{company_folder}"):
                os.rename(f"../Storage/Companies/{company_folder}/{csv_file}", f"../Storage/Companies_csvs/{csv_file}")
                print("        " + csv_file)

companies_csv_files = os.listdir("../Storage/Companies_csvs")

print("After processing ...")
print("Number of companies csv files:", len(companies_csv_files))
print()
