# Set Similarity Search with PySpark

This project implements a scalable solution using PySpark to identify highly similar itemsets within large transaction datasets. It leverages Jaccard similarity and prefix filtering to optimize the search.

## 📦 Files Included

- `project3.py`: Main PySpark script for similarity computation.
- `testcase1.csv`, `testcase2.csv`: Sample input datasets.
- `testcase1_output(tau=0.7)`, `testcase2_output(tau=0.8)`: Output result files.
- `my_testcase.csv`, `sample.csv`: Additional custom input data.
- `running_time`: Log file tracking performance.

## 🚀 Run Instructions

You can run the script with Spark as follows:

```bash
spark-submit project3.py testcase1.csv output_dir 0.7
```

- `testcase1.csv`: Input dataset
- `output_dir`: Folder where output files will be stored
- `0.7`: Similarity threshold (`tau`)

## 📌 Project Overview

- **Goal**: Efficiently identify itemset pairs with high Jaccard similarity using big data techniques.
- **Technologies**: PySpark, Jaccard Similarity, Prefix Filtering
- **Key Concepts**: Set-based similarity join, performance tuning for distributed data processing

## 🔍 Features

- Prefix Filtering for Jaccard optimization
- Frequency-based item reordering
- Designed for scalable distributed computing

---

## 👨‍💻 Author

Po-Hsun Chang  
Master of Information Technology (Database Systems)  
University of New South Wales  
📧 chris89019@gmail.com  
🔗 [LinkedIn](https://linkedin.com/in/pohsunchang)
