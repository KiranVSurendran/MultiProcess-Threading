import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from PIL import Image

def convert_and_move_file(j2k_file_path):
    try:
        jpg_folder = os.path.join(os.path.dirname(j2k_file_path), 'JPG')
        j2k_folder = os.path.join(os.path.dirname(j2k_file_path), 'J2K')

        os.makedirs(jpg_folder, exist_ok=True)
        os.makedirs(j2k_folder, exist_ok=True)

        jpg_file_path = os.path.join(jpg_folder, os.path.splitext(os.path.basename(j2k_file_path))[0] + '.jpg')

        img = Image.open(j2k_file_path)
        img.save(jpg_file_path, 'JPEG')

        os.replace(j2k_file_path, os.path.join(j2k_folder, os.path.basename(j2k_file_path)))
        print(f"Converted and moved: {j2k_file_path} -> {jpg_file_path}")

    except Exception as e:
        print(f"Error processing {j2k_file_path}: {e}")
        move_invalid_file(j2k_file_path)

def move_invalid_file(j2k_file_path):
    invalid_folder = os.path.join(os.path.dirname(j2k_file_path), 'J2K_INVALID')
    os.makedirs(invalid_folder, exist_ok=True)

    os.replace(j2k_file_path, os.path.join(invalid_folder, os.path.basename(j2k_file_path)))
    print(f"Moved to J2K_INVALID: {j2k_file_path}")

def process_folder(root):
    with ThreadPoolExecutor() as executor:
        futures = []

        for file in os.listdir(root):
            file_path = os.path.join(root, file)

            if os.path.isfile(file_path) and file.lower().endswith('.j2k'):
                futures.append(executor.submit(convert_and_move_file, file_path))

        for future in futures:
            future.result()

def convert_and_move_files(input_folder, folder_name):
    with ProcessPoolExecutor() as executor:
        futures = []

        for dirs in os.listdir(input_folder):
            root =  os.path.join(input_folder, dirs, folder_name)
            if os.path.isdir(root):
                futures.append(executor.submit(process_folder,root))

        for future in futures:
            future.result()


if __name__ == "__main__":
    folder_name = "Images"
    folder_path = "/home/kinnan/Projects/Datasets/test_new"
    convert_and_move_files(folder_path)
