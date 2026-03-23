# Marketplace Visibility Pipeline

## Overview

This project is a Python-based data pipeline for collecting marketplace visibility data from platforms such as **Back Market** and **Kaufland**, storing the results in **PostgreSQL**, and using the collected data for simple ranking and visibility analysis.

The main idea behind this project is to understand how product visibility changes across marketplace search results for specific keywords such as:

- PlayStation
- Nintendo
- iPhone
- other product-related search terms

For each keyword, the pipeline collects product results, stores structured data in PostgreSQL, and prepares it for later analysis such as:

- rank tracking over time
- visibility changes by keyword
- sponsored vs non-sponsored result comparison
- platform-level comparison
- product-level trend monitoring

This project started as a practical internal automation effort and is now being refactored into a cleaner public portfolio project focused on:

- data collection
- Python automation
- PostgreSQL integration
- data pipeline design
- analysis foundations
- future database administration and application development

---

## Project Idea

When users search for a keyword on an online marketplace, the ranking and visibility of products can change over time.

This project aims to answer questions like:

- Which products appear in the top search results for a keyword?
- How does a product rank change over time?
- Are some products sponsored?
- How does visibility differ across marketplaces?
- Can the collected data be stored in a structured way for reporting and later analytics?

Instead of only scraping the visible frontend, this project explores a more technical workflow:

1. identify how the marketplace frontend retrieves its data
2. call the backend request directly when possible
3. fall back to browser automation when direct requests are blocked
4. transform the data into a structured dataframe
5. insert the results into PostgreSQL
6. build simple analysis and reporting on top of the stored data

This makes the project more than a crawler. It becomes a small **data ingestion and visibility tracking platform**.

---

## Current Features

### Data collection
- Collects visibility data from marketplace search results
- Supports keyword-based queries
- Retrieves up to the first 100 results depending on platform behavior
- Handles different collection strategies depending on the platform

### PostgreSQL integration
- Reads keyword inputs from PostgreSQL
- Inserts crawled visibility data into PostgreSQL tables
- Includes helper scripts for bulk keyword insertion and other database operations

### Analysis
- Basic rank tracking for individual products over time
- Simple matplotlib visualization for rank changes

### Refactoring in progress
The repo is currently being cleaned and reorganized into a better package structure so it is easier to:
- understand
- run locally
- maintain
- expand into a larger project

---

## Why I Built This

I wanted to build something practical that combines:

- Python scripting
- web data collection
- data transformation
- PostgreSQL usage
- analytics preparation

I was also interested in understanding how marketplace search visibility works in practice.

A second motivation was to move beyond simple one-off scripts and start building something closer to a reusable data pipeline. This repo is part of that transition.

---

## Technical Approach

The pipeline follows this general flow:

### 1. Keyword source
Keywords are stored in PostgreSQL and used as the input for the collection process.

### 2. Marketplace-specific collection
Each platform has its own collection logic.

Examples:
- **Back Market**: direct backend/API-style request based on frontend network inspection
- **Kaufland**: tries backend request first, with Selenium fallback when bot protection blocks direct access

### 3. Transformation
The collected raw response data is transformed into structured tabular data with fields such as:

- keyword
- product link
- offer ID
- brand
- product title
- rank
- sponsored flag
- platform ID
- EAN
- crawl date

### 4. Database insertion
The transformed dataframe is inserted into PostgreSQL using helper functions in the database layer.

### 5. Analysis
Stored data can then be queried again for:
- trend analysis
- product-level rank monitoring
- future reporting and dashboards

---

## Project Structure

```text
marketplace-visibility-pipeline/
│
├── src/
│   ├── database/
│   │   ├── db_connection.py
│   │   ├── db_select.py
│   │   ├── db_insert_visibility.py
│   │   ├── db_insert_keywords.py
│   │   ├── db_insert_orders.py
│   │   └── db_insert_drt.py
│   │
│   ├── platforms/
│   │   ├── backmarket_va_weekly.py
│   │   ├── backmarket_rank.py
│   │   └── kaufland_va_weekly.py
│   │
│   └── utils/
│       ├── file_operation.py
│       └── excel_formula.py
│
├── docs/
├── sample_data/
├── .env.example
├── .gitignore
└── README.md
