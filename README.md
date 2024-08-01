Here's the updated README file with the mention of using Scrapy:

---

# Web Crawling and URL Data Analysis

This repository contains the code and analysis from my first task during my internship at PayPal. The primary objective of this task was to learn the fundamentals of web crawling and explore the data world of URLs. The insights gained from this project have provided a foundational understanding necessary for more advanced tasks.

## Project Overview

The task involved conducting an exploratory data analysis (EDA) on a set of seed URLs. The main objectives were to:

1. **Identify the countries of origin for the URLs.**
2. **Classify the domains, such as educational, non-profit, or technological.**

To accomplish these tasks, I used **Scrapy**, a powerful web crawling framework in Python, to build a crawler that fetched sublinks (one level down) from the seed URLs. The analysis of these sublinks included:

1. **Categorizing the types of webpages found.**
2. **Identifying commonly used delimiters in the URLs.**
3. **Counting the number of sublinks for each seed URL.**

### Additional Insights

- **Average Length of Sublinks:** Calculation of the average length of the sublinks for each seed URL.
- **Common Page Types Visualization:** Visualization of the most common types of pages encountered.
- **Domain Analysis:** Breakdown of the most frequent top-level domains (TLDs) and their purposes or corresponding countries.


## Repository Structure

- **data/**: Contains the raw data used in the analysis.
- **mycrawler/**: Includes the Scrapy crawler files.
- **notebooks**: Jupyter notebook with detailed exploratory data analysis and visualizations.
- **.csv files**: CSV files containing the output of the crawler.
- **README.md**: This file, providing an overview of the project.


## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.


## Acknowledgments

- Special thanks to the team at PayPal for their guidance and support during this project.

---
