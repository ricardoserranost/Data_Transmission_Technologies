import os
import random
import shutil
import csv
import cv2

# ===== CONFIGURATION =====
input_folder = "images_4k"       # Your existing 4K images folder
output_1080p = "images_1080p"
output_720p = "images_720p"
sample_count = 300               # Number of images to randomly sample
jpeg_quality = 85                # JPEG quality (1-100)

# Target resolutions
res_1080p = (1920, 1080)
res_720p = (1280, 720)

# Supported image extensions
valid_exts = (".jpg", ".jpeg", ".png", ".bmp", ".tiff")

# ===== PREPARE OUTPUT =====
os.makedirs(output_1080p, exist_ok=True)
os.makedirs(output_720p, exist_ok=True)

# ===== LIST AND SAMPLE IMAGES =====
all_images = [f for f in os.listdir(input_folder) if f.lower().endswith(valid_exts)]
if len(all_images) < sample_count:
    raise ValueError(f"Not enough images ({len(all_images)}) to sample {sample_count}")
sampled_images = random.sample(all_images, sample_count)

# ===== MANIFEST =====
manifest_file = "image_manifest.csv"
with open(manifest_file, mode="w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["filename", "resolution", "filesize_bytes"])  # header

    for fname in sampled_images:
        src_path = os.path.join(input_folder, fname)
        img = cv2.imread(src_path)
        if img is None:
            print(f"Could not read {fname}, skipping.")
            continue

        # Resize 1080p
        img_1080p = cv2.resize(img, res_1080p, interpolation=cv2.INTER_AREA)
        out_1080p = os.path.join(output_1080p, fname)
        cv2.imwrite(out_1080p, img_1080p, [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality])
        writer.writerow([fname, "1080p", os.path.getsize(out_1080p)])

        # Resize 720p
        img_720p = cv2.resize(img, res_720p, interpolation=cv2.INTER_AREA)
        out_720p = os.path.join(output_720p, fname)
        cv2.imwrite(out_720p, img_720p, [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality])
        writer.writerow([fname, "720p", os.path.getsize(out_720p)])

        print(f"Processed {fname}")

print(f"\n Done! Manifest saved to '{manifest_file}'")

