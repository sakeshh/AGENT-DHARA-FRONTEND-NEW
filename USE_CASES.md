# Data Pipeline Use Cases & Examples

## 📋 Table of Contents
1. [E-commerce Analytics](#1-e-commerce-analytics)
2. [Healthcare Data Cleaning](#2-healthcare-data-cleaning)
3. [Financial Reporting](#3-financial-reporting)
4. [IoT Sensor Data Processing](#4-iot-sensor-data-processing)
5. [Social Media Analytics](#5-social-media-analytics)

---

## 1. E-commerce Analytics

### Scenario
An online retailer needs to clean customer order data with duplicates and missing values before analysis.

### Workflow

**Step 1: Database Selection**
- Select: `MySQL Analytics` database

**Step 2: File Selection**
- Select files:
  - `customer_orders` (67,890 rows)
  - `product_catalog` (8,930 rows)
  - `customer_demographics` (23,450 rows)

**Step 3: Assessment Results**
```
customer_orders:
  - Quality Score: 72%
  - Duplicates: 3,456 (5.1%)
  - Missing Values: 2,341 (email field mostly)
  - Issues: 
    - High: Email field 34% null
    - Medium: Order dates inconsistent format
    - Low: Customer IDs have leading zeros

product_catalog:
  - Quality Score: 89%
  - Duplicates: 124
  - Missing Values: 456 (description field)

customer_demographics:
  - Quality Score: 81%
  - Missing Values: 1,234 (age field)
```

**Step 4: Report Format**
- Selected: **Excel** (for business stakeholders)

**Step 5: ETL Code**
- Selected: **Python/Pandas** (data science team prefers)
- Generated code includes:
  - Email validation and cleanup
  - Date standardization to ISO format
  - Customer ID zero-padding removal
  - Product description filling from related tables

**Step 6: Data Cleaning**
- Confirmed: Yes, clean the data
- Results:
  ```
  customer_orders_cleaned:
    Original: 67,890 rows
    Cleaned: 64,434 rows
    Duplicates Removed: 3,456
    Missing Values Handled: 2,341
    Blob URL: https://storage.blob.../customer_orders_cleaned_1234567890.csv
  ```

**Outcome**: Clean dataset ready for sales analytics and ML models.

---

## 2. Healthcare Data Cleaning

### Scenario
Hospital needs to consolidate patient records from multiple systems with duplicate entries.

### Workflow

**Step 1: Database Selection**
- Select: `PostgreSQL Production` database

**Step 2: File Selection**
- Select files:
  - `patient_records` (102,340 rows)
  - `medical_history` (45,230 rows)
  - `prescriptions` (89,340 rows)

**Step 3: Assessment Results**
```
patient_records:
  - Quality Score: 65%
  - Duplicates: 8,234 (8%)
  - Missing Values: 5,678
  - Issues:
    - High: Duplicate patient IDs from system migration
    - High: SSN field partially encrypted, partially plain
    - Medium: Birth dates in multiple formats
    - Medium: Missing insurance information
```

**Feedback**: User dislikes initial assessment
- Comment: "SSN encryption detection is incorrect"
- Agent re-assesses with improved encryption detection
- New Quality Score: 68% (more accurate)

**Step 4: Report Format**
- Selected: **PDF** (for compliance audit)

**Step 5: ETL Code**
- Selected: **Python/Pandas** with added HIPAA compliance features
- Special handling for PHI (Protected Health Information)

**Step 6: Data Cleaning**
- Confirmed: Yes, with extra confirmation for sensitive data
- Results with audit trail:
  ```
  patient_records_cleaned:
    Original: 102,340 rows
    Cleaned: 94,106 rows
    Duplicates Removed: 8,234
    Missing Values Handled: 5,678
    Blob URL: [Encrypted endpoint]
    Audit Log: Available for compliance review
  ```

**Outcome**: HIPAA-compliant cleaned dataset with full audit trail.

---

## 3. Financial Reporting

### Scenario
Financial institution needs to prepare quarterly transaction data for regulatory reporting.

### Workflow

**Step 1: Database Selection**
- Select: `SQL Server Sales` database

**Step 2: File Selection**
- Select files:
  - `transactions` (156,789 rows)
  - `accounts` (34,567 rows)
  - `balances` (34,567 rows)

**Step 3: Assessment Results**
```
transactions:
  - Quality Score: 94%
  - Duplicates: 234 (0.15%)
  - Missing Values: 45
  - Issues:
    - Medium: Some transaction amounts have >2 decimal places
    - Low: Currency codes inconsistent (USD vs $ vs US)
    - Low: Timezone not specified for timestamps
```

**Step 4: Report Format**
- Selected: **Excel** + **PDF** (both needed for reporting)

**Step 5: ETL Code**
- Selected: **PySpark** (large dataset, distributed processing)
- Code includes:
  - Decimal precision standardization
  - Currency code normalization
  - Timezone conversion to UTC
  - Transaction validation rules

**Step 6: Data Cleaning**
- User chooses: "No, Code is Enough"
- Rationale: Internal team will run code with custom validations

**Outcome**: Production ETL code ready for automated quarterly runs.

---

## 4. IoT Sensor Data Processing

### Scenario
Manufacturing plant needs to clean sensor data with missing readings and outliers.

### Workflow

**Step 1: Database Selection**
- Select: `MongoDB Logs` database

**Step 2: File Selection**
- Select files:
  - `temperature_sensors` (2,345,678 documents)
  - `pressure_sensors` (1,987,654 documents)
  - `vibration_sensors` (3,456,789 documents)

**Step 3: Assessment Results**
```
temperature_sensors:
  - Quality Score: 76%
  - Missing Values: 123,456 (sensor downtime)
  - Outliers: 45,678 (impossible values)
  - Issues:
    - High: 5.3% of readings show sensor malfunction (999 values)
    - Medium: Timestamp gaps during maintenance windows
    - Low: Some readings in Fahrenheit instead of Celsius
```

**Feedback**: User dislikes outlier detection
- Comment: "999 is valid sensor error code, not outlier"
- Agent re-learns and updates outlier rules
- Re-assessment respects 999 as error code

**Step 4: Report Format**
- Selected: **JSON** (for integration with analytics platform)

**Step 5: ETL Code**
- Selected: **PySpark** (big data volumes)
- Special features:
  - Time-series interpolation for missing values
  - Outlier detection with configurable thresholds
  - Unit conversion (F to C)
  - Error code preservation

**Step 6: Data Cleaning**
- Confirmed: Yes, clean with learned preferences
- AI applies previous feedback about error codes
- Results:
  ```
  temperature_sensors_cleaned:
    Original: 2,345,678 docs
    Cleaned: 2,222,222 docs
    Invalid Removed: 123,456
    Interpolated: 67,890
    Blob URL: https://storage.blob.../temp_sensors_cleaned_[timestamp].json
  ```

**Outcome**: Clean sensor data ready for predictive maintenance ML models.

---

## 5. Social Media Analytics

### Scenario
Marketing agency analyzing social media engagement data with duplicates and spam.

### Workflow

**Step 1: Database Selection**
- Select: `PostgreSQL Development` database

**Step 2: File Selection**
- Select files:
  - `social_posts` (456,789 rows)
  - `user_engagement` (1,234,567 rows)
  - `comments` (987,654 rows)

**Step 3: Assessment Results**
```
social_posts:
  - Quality Score: 68%
  - Duplicates: 23,456 (5.1% - reposts counted)
  - Spam: ~12% detected
  - Issues:
    - High: Duplicate posts across platforms
    - High: Bot-generated spam content
    - Medium: Hashtags inconsistent (#tag vs tag)
    - Low: Emoji encoding issues

user_engagement:
  - Quality Score: 82%
  - Duplicates: 45,678 (3.7%)
  - Issues:
    - Medium: User IDs from different platforms not normalized
```

**Feedback Loop**:
1. Initial assessment: User dislikes spam detection accuracy (👎)
   - Comment: "Too many false positives on promotional content"
2. Agent adjusts spam detection rules
3. Re-assessment: Better accuracy (👍)

**Step 4: Report Format**
- Selected: **HTML** (for interactive dashboard)

**Step 5: ETL Code**
- Selected: **Node.js** (JavaScript team, React dashboard)
- Features:
  - Platform-specific user ID normalization
  - Improved spam detection based on feedback
  - Hashtag standardization
  - Emoji encoding fixes (UTF-8)
  - Engagement metrics calculation

**Step 6: Data Cleaning**
- Confirmed: Yes, clean the data
- Results:
  ```
  social_posts_cleaned:
    Original: 456,789 rows
    Cleaned: 401,234 rows
    Duplicates Removed: 23,456
    Spam Removed: 32,099
    Blob URL: https://storage.blob.../social_posts_cleaned_[timestamp].csv

  user_engagement_cleaned:
    Original: 1,234,567 rows
    Cleaned: 1,188,889 rows
    Duplicates Removed: 45,678
    Blob URL: https://storage.blob.../engagement_cleaned_[timestamp].csv
  ```

**Outcome**: Clean engagement data with reduced false positives, ready for sentiment analysis.

---

## 🎯 Key Takeaways

### When to Use Each Language:

| Language | Best For | Example Use Case |
|----------|----------|------------------|
| **Python/Pandas** | Medium datasets, data science | E-commerce, Healthcare |
| **PySpark** | Big data, distributed processing | Financial, IoT sensors |
| **Node.js** | JavaScript environments, APIs | Social media, Real-time apps |

### Report Format Selection:

| Format | Best For | Example Use Case |
|--------|----------|------------------|
| **PDF** | Compliance, archiving | Healthcare, Financial reports |
| **Excel** | Business stakeholders, analysis | E-commerce, Marketing |
| **JSON** | API integration, automation | IoT, Social media |
| **HTML** | Interactive dashboards | Social media, Monitoring |
| **CSV** | Universal compatibility, imports | General purpose |

### Feedback Learning Examples:

1. **Spam Detection**: Initially too aggressive → User feedback → Adjusted sensitivity
2. **Outlier Rules**: Misidentified error codes → User correction → Learned exceptions
3. **Encryption Detection**: Incorrect in healthcare → User feedback → Improved algorithm
4. **Assessment Metrics**: User preferences for specific KPIs → Personalized reports

---

## 💡 Pro Tips

1. **Start with Assessment**: Always review quality scores before cleaning
2. **Provide Detailed Feedback**: Specific comments help AI learn better
3. **Test on Subset First**: Select 1-2 files initially to verify process
4. **Save Blob URLs**: Keep track of cleaned file locations for future reference
5. **Iterate**: Use feedback loop to refine results until satisfied
6. **Export Code**: Download generated ETL code for version control
7. **Document Decisions**: Keep notes on why certain cleaning steps were chosen

---

**Ready to process your data? Start your pipeline now!** 🚀
