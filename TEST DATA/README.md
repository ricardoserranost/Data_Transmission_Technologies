# Image Downsampling

This script downsamples 4K images into **1080p** and **720p** JPEGs and generates a CSV manifest with metadata. This was used during the tests to obtain the datasets from the original 4k images.

## Features
- Resizes to **1920×1080** and **1280×720**  
- Saves as JPEG with configurable quality (default `85`)  
- Creates `image_manifest.csv` with filename, resolution, and file size  

## Requirements
pip install opencv-python

## Usage:
- Place images in "images_4k" folder
- Edit config at the top of the script: samples, jpeg, output folders
- Run in terminal:
```bash
python downsamplefrom4k.py