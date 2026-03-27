// Bulk upload all images in a folder to Cloudinary and print URLs
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const cloudinary = require("cloudinary").v2;

// Configure Cloudinary with your credentials from .env
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// Path to your images folder
const IMAGES_DIR = path.join(__dirname, "../frontend/public/assets/images/products");

fs.readdir(IMAGES_DIR, async (err, files) => {
  if (err) {
    console.error("❌ Error reading directory:", err);
    return;
  }
  // Filter for PNG, JPG, JPEG, GIF files
  const imageFiles = files.filter(f =>
    /\.(png|jpg|jpeg|gif)$/i.test(f)
  );
  console.log(`Found ${imageFiles.length} images to upload.\n`);
  for (const file of imageFiles) {
    const filePath = path.join(IMAGES_DIR, file);
    try {
      const result = await cloudinary.uploader.upload(filePath, {
        folder: "products", // Cloudinary folder
        use_filename: true,
        unique_filename: false,
        overwrite: false,
      });
      console.log(`${file} ➡️ ${result.secure_url}`);
    } catch (uploadErr) {
      console.error(`❌ Failed to upload ${file}:`, uploadErr.message);
    }
  }
  console.log("\n✅ All done!");
});