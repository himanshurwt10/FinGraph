# FinGraph — Graph-Based Financial Transaction Analysis

FinGraph is a data analytics project that uses graph technology to analyze financial transactions and identify suspicious patterns in synthetic data.

## Tech Stack
- **Python** — Transaction data generation
- **Neo4j & Cypher** — Graph modeling and querying
- **Neo4j GDS** — PageRank, WCC, and Louvain analysis
- **Power BI** — Interactive dashboard and visualization

## Key Features
- Models accounts as nodes and transactions as relationships.
- Uses PageRank to identify influential accounts.
- Applies WCC to analyze network connectivity.
- Uses Louvain to detect account communities.
- Investigates a simulated starburst transaction pattern.

## Key Findings
- **121 accounts** analyzed
- **1 connected component**
- **4 communities** detected
- **25 senders** in the simulated starburst pattern
- **₹2,47,500** total received in the simulation

## Project Workflow
1. Generate synthetic transaction data.
2. Import data into Neo4j.
3. Perform graph analytics using Cypher and GDS.
4. Export results to CSV.
5. Build a Power BI dashboard.

## Disclaimer
This project uses synthetic data for educational purposes. Its findings do not establish real-world financial fraud.

## Author
**Himanshu Rawat**  
[GitHub](https://github.com/himanshurwt10)
