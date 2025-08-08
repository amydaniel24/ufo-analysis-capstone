
# UFO and Drone Sightings Analysis

## Project Overview

This project analyzes reported sightings of UFOs and drones across the United States. The goal is to see if there are patterns in the data and how proximity to national parks or population density plays a role.

## Problem Statement

Man has always looked to the stars for answers, but these days, there are lots of questions about what we are seeing. Public interest in UFO's and Drones has increased rapidly in the last several years. This project aims to investigate:
- Where and when these sightings occur most frequently.
- Whether there's a geographic overlap between UFO and drone sightings.
- If sightings cluster around national parks or in more populated areas.

## Goals and Objectives

1. Clean and merge data from multiple sources (UFOs, drones, national parks, and population density).
2. Explore trends over time and geography.
3. Visualize patterns across cities and states.
4. Examine correlations between sightings and proximity to national parks.
5. Present findings in a clear, interactive notebook with visuals.

## Data Sources

- UFO Sightings: [NUFORC Data via Kaggle](https://www.kaggle.com/datasets/NUFORC/ufo-sightings)
- Drone Sightings: [FAA Drone Reports](https://www.faa.gov/uas/resources/public_records/uas_sightings_report)
- National Parks: [National Park Service API](https://www.nps.gov/subjects/developer/api-documentation.htm)
- Population Data: [U.S. Census Data](https://www.census.gov/data.html)

## Technologies Used

- Python
- Pandas, NumPy
- Plotly, Matplotlib, Seaborn
- Jupyter Notebook (VS Code)
- Git and GitHub

## Project Structure

```
ufo-analysis-capstone/
├── data/
│   ├── [CSV files for each dataset]
├── notebooks/
│   ├── final_capstone_notebook.ipynb
│   └── final_capstone_notebook.html
├── README.md
├── requirements.txt
```

## How to Run This Project

### Option 1: Run on Your Local Machine

#### 1. Clone the Repository
```bash
git clone https://github.com/amydaniel24/ufo-analysis-capstone.git
cd ufo-analysis-capstone
```

#### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 3. Launch the Notebook
```bash
cd notebooks
jupyter notebook final_capstone_notebook.ipynb
```

Or open the `.ipynb` file in VS Code with the Python extension installed.

---

### Option 2: Run Online (No Installation Required)

1. Go to [Google Colab](https://colab.research.google.com/)
2. Click **"GitHub"** tab and enter: `amydaniel24/ufo-analysis-capstone`
3. Select `final_capstone_notebook.ipynb`
4. Click **"Run All"** (ensure you upload the required data files if prompted)

---

## Author

Amy Daniel  
[GitHub Profile](https://github.com/amydaniel24)

---

## Summary

This project showcases exploratory data analysis and geospatial insights into UFO and drone sightings across the U.S., integrating multiple datasets and using data visualization to reveal patterns worth further investigation.
