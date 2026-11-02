# FinGraph – Real-Time Financial Fraud Detection System

FinGraph is a real-time financial transaction monitoring and fraud detection project. It uses graph analytics and stream processing to identify suspicious transaction patterns, monitor high-risk accounts, and generate fraud alerts.

The project uses synthetic transaction data to simulate financial activities.

## 🚀 Features

- **Synthetic Transaction Generator:** Generates continuous financial transactions.
- **Real-Time Streaming:** Uses Apache Kafka to stream transactions.
- **Stream Processing:** Uses Apache Flink to process incoming transactions.
- **Graph Database:** Stores accounts and transaction relationships in Neo4j AuraDB.
- **Fraud Detection:** Identifies suspicious activities such as starburst transactions and circular transaction patterns.
- **Risk Scoring:** Assigns risk scores and risk levels to accounts.
- **Graph Analytics:** Uses PageRank, Weakly Connected Components (WCC), and Louvain community detection.
- **Fraud Alerts:** Generates alerts for high-risk accounts.
- **Interactive Dashboard:** Displays live transaction data, account details, fraud alerts, and network visualizations using Streamlit.

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core programming and transaction generation |
| Apache Kafka | Real-time data streaming |
| Apache Flink | Stream processing |
| Neo4j AuraDB | Graph database |
| Cypher | Graph queries |
| Neo4j GDS | Graph analytics |
| Streamlit | Interactive dashboard |
| Plotly | Data visualization |
| NetworkX | Network graph visualization |
| Pandas | Data processing |

## 🏗️ Project Architecture

```text
Python Transaction Producer
          |
          v
    Apache Kafka
   (transactions)
          |
          v
     Apache Flink
   (Stream Processing)
          |
          v
    Apache Kafka
(transactions_processed)
          |
          v
     Neo4j Sink
          |
          v
     Neo4j AuraDB
          |
          v
   Streamlit Dashboard
```

## 🔍 Fraud Detection Techniques

FinGraph focuses on identifying suspicious transaction patterns:

- **Starburst Transactions:** Detects multiple accounts sending money to a common account.
- **Circular Transactions:** Identifies circular money transfer patterns.
- **Risk Scoring:** Calculates account risk scores based on suspicious activity.
- **Graph Analytics:** Analyzes account relationships and network structures using graph algorithms.

## 📊 Dashboard

The Streamlit dashboard provides the following sections:

- **Overview:** Key transaction and account metrics.
- **Fraud Intelligence:** Suspicious activities and fraud alerts.
- **Network Graph:** Visual representation of account relationships.
- **Transaction Explorer:** Search and explore transactions.
- **Account 360° View:** Detailed account activity and transaction history.
- **Fraud Alert Management:** Manage alert statuses.

## ⚙️ Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/himanshurwt10/FinGraph.git
cd FinGraph
```

### 2. Create a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```env
AURA_URI=your_neo4j_aura_uri
AURA_USERNAME=your_neo4j_aura_username
AURA_PASSWORD=your_neo4j_aura_password
NEO4J_DATABASE=your_database_name
```

Use your own Neo4j AuraDB credentials. Never upload your `.env` file to GitHub.

### 5. Start the Streaming Pipeline

Start each component in a separate terminal.

**Terminal 1 – Kafka**

```bash
brew services start kafka
```

**Terminal 2 – Flink Processor**

```bash
cd ~/Documents/FinGraph
source .venv-flink/bin/activate
python flink/transaction_processor.py
```

**Terminal 3 – Neo4j Sink**

```bash
cd ~/Documents/FinGraph
source .venv/bin/activate
python flink/neo4j_sink.py
```

**Terminal 4 – Transaction Producer**

```bash
cd ~/Documents/FinGraph
source .venv/bin/activate
python streaming/transaction_producer.py
```

### 6. Run the Dashboard Locally

Open another terminal:

```bash
cd ~/Documents/FinGraph
source .venv/bin/activate
streamlit run dashboard/app.py
```

The dashboard will open at:

```text
http://localhost:8501
```

The deployed dashboard can also be accessed through its Streamlit Community Cloud URL.

## 📁 Project Structure

```text
FinGraph/
│
├── dashboard/
│   └── app.py
│
├── streaming/
│   └── transaction_producer.py
│
├── flink/
│   ├── transaction_processor.py
│   └── neo4j_sink.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── .gitignore
├── requirements.txt
└── README.md
```

*Additional project files and data-processing scripts are maintained in their respective folders.*

## ⚠️ Important Note

This project uses synthetic financial transaction data for demonstration and educational purposes. Fraud indicators and risk scores are simulated and should not be treated as confirmed financial crimes.

The streaming pipeline currently runs locally, while the dashboard is deployed on Streamlit Community Cloud and connected to Neo4j AuraDB.

## 👨‍💻 Author

**Himanshu Rawat**

B.Tech – Computer Science and Engineering

GitHub: [himanshurwt10](https://github.com/himanshurwt10)

LinkedIn: [Himanshu Rawat](https://www.linkedin.com/in/himanshu-rawat-522375272/)