# PhonePe Pulse Data Visualization & Analysis (2018–2024)

## 📱 Project Overview
This project is a comprehensive data science solution designed to extract, process, and visualize data from the **PhonePe Pulse** open-source repository. It provides deep insights into the digital payment landscape in India, including transaction trends, user demographics, and insurance analytics across various states, districts, and pincodes.

## 🚀 Features
- **ETL Pipeline**: Automated extraction of raw JSON data into a structured PostgreSQL database.
- **Interactive Dashboard**: A user-friendly Streamlit web application with dynamic filters.
- **Multi-dimensional Analysis**:
  - **Transactions**: Value and count analysis by state and category.
  - **Users**: Market share of mobile brands and user engagement metrics.
  - **Insurance**: Penetration and growth of insurance products.
  - **Top Performers**: Identification of top-performing districts and pincodes.
  - **Trend Analysis**: Yearly growth patterns from 2018 to 2024.
- **Geographical Insights**: Analysis at the State, District, and Pincode levels.

## 🛠️ Technology Stack
- **Language**: Python 3.10+
- **Database**: PostgreSQL
- **Web Framework**: Streamlit
- **Visualization**: Plotly, Pandas
- **Database Toolkit**: SQLAlchemy, Psycopg2
- **Data Source**: [PhonePe Pulse GitHub Repository](https://github.com/PhonePe/pulse)

## 🏗️ Data Architecture (ETL)
The project follows a standard Data Engineering workflow:
1. **Extraction**: Scripted navigation through thousands of nested JSON files.
2. **Transformation**: Cleaning and flattening data into tabular formats using Pandas.
3. **Loading**: Automated ingestion into 9 structured SQL tables in PostgreSQL.

## 📂 Project Structure
```text
├── data/                   # Raw JSON data from PhonePe Pulse
├── ingest_data.py          # ETL script for data ingestion
├── analysis.py             # SQL-based analytical queries
├── app.py                  # Streamlit dashboard application
├── generate_documentation.py # Documentation generator
├── requirements.txt        # Project dependencies
└── README.md               # Project overview
```

## ⚙️ Setup & Installation
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/phonepe-pulse-analysis.git
   cd phonepe-pulse-analysis
   ```
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Database Setup**:
   - Install PostgreSQL.
   - Create a database named `phonepe_pulse`.
   - Update the connection credentials in `ingest_data.py` and `app.py`.
4. **Ingest Data**:
   ```bash
   python ingest_data.py
   ```
5. **Run the Dashboard**:
   ```bash
   streamlit run app.py
   ```

## 📊 Dashboard Insights
The interactive dashboard provides **16 distinct visualizations** across 5 tabs:
- **Transaction Analysis**: Deep dive into payment categories and state performance.
- **User Analysis**: Understanding the device ecosystem and registration trends.
- **Insurance Analysis**: Tracking the adoption of digital insurance.
- **Top Charts**: Rankings of top districts and pincodes.
- **Trend Analysis**: Long-term line charts showing the evolution of UPI in India.

## 💡 Key Findings
- **UPI Growth**: Exponential increase in transaction value post-2020.
- **Merchant Adoption**: A significant shift from P2P to Merchant payments for daily utility.
- **Regional Leaders**: Southern states (Telangana, Karnataka) lead in digital adoption volume.
- **Device Ecosystem**: Xiaomi and Samsung users represent the largest segment of PhonePe users.

## 🤝 Contributing
Contributions are welcome! Feel free to open an issue or submit a pull request for any improvements or new features.

## 📄 License
This project is licensed under the MIT License.
