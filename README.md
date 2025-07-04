# 🔍 Data Quality Checker (FastAPI + Streamlit)

A lightweight, YAML-configurable data quality validation tool built with FastAPI, Pandas, and Streamlit. Upload your CSV, define your own rules, and get a downloadable Excel report highlighting:
- ❌ Nulls
- 📏 Range violations
- 🔁 Duplicate records

---

## 🚀 Live App

👉 [Try the Streamlit App](https://data-pipeline-project-9vj4tmufznxwkpekveiryg.streamlit.app/)

---

## 📂 Features

- Upload any CSV file
- Edit validation rules in YAML (no coding required)
- Instant Excel report generation
- FastAPI backend with `/run-checks` API
- Streamlit frontend for user-friendly interaction

---

## 🛠️ Tech Stack

- Python 3.10+
- FastAPI
- Pandas
- Streamlit
- PyYAML
- Render (for FastAPI deployment)
- Streamlit Cloud (for frontend deployment)

---

## 🧪 Example Rules (YAML)

```yaml
columns:
  age:
    min: 18
    max: 60
    required: true
  name:
    required: true
