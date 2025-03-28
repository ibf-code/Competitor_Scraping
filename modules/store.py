import os
import csv

def save_to_csv(data, filename, subdir="mayesh", output_root="output"):
    if not data:
        raise ValueError("no data to save")

    output_dir = os.path.join(output_root, subdir)
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, filename)

    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"saved {filename}")
    return file_path