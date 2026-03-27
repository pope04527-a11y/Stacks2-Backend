const express = require('express');
const router = express.Router();
const upload = require('../cloudinaryMulter'); // path to your multer setup

router.post('/upload', upload.single('image'), (req, res) => {
  // req.file.path contains the cloudinary image URL
  res.json({ imageUrl: req.file.path });
});

module.exports = router;