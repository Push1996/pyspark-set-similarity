# PySpark Set Similarity Detection

This project implements a scalable set similarity detection system using Apache PySpark.  
The goal is to identify transaction pairs across different years with high **Jaccard similarity**.  
It was originally developed as part of a big data coursework project at UNSW.

---

## 🔧 Technologies
- Python 3
- Apache PySpark
- Jaccard Similarity
- Prefix Filtering
- Frequency-based Item Ordering

---

## 📌 Key Features

- Loads and tokenizes transactional datasets from CSV.
- Computes token frequency and applies global frequency-based ordering.
- Filters candidate pairs using prefix filtering based on a similarity threshold (`tau`).
- Computes **Jaccard similarity** only across **cross-year transaction pairs**.
- Outputs top similar transaction pairs sorted by transaction ID.

---

## 🚀 How to Run

Make sure you have PySpark installed and set up.

### Sample Command:
```bash
spark-submit project3.py testcase1.csv output_dir 0.7
