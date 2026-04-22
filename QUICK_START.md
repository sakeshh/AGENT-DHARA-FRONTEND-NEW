# Quick Start Guide - Data Pipeline Workflow

## 🚀 Getting Started

### 1. Access the Application
Navigate to the chat interface and click the **"Data Pipeline"** button in the sidebar, or go directly to `/data-pipeline`

### 2. Complete Workflow Steps

#### **Step 1: Select Database**
- Browse through available databases
- Use the search bar to filter by name or type
- Click on a database card to select it
- Supported: PostgreSQL, MySQL, MongoDB, SQL Server, Oracle

#### **Step 2: Choose Files**
- View all tables/collections in the selected database
- See metadata: size, row count, and type
- Select single or multiple files (multi-select)
- Click "Continue" when ready

#### **Step 3: Data Assessment**
- Wait for automatic analysis (progress bar shown)
- Review the assessment report:
  - ✅ Data quality score
  - 📊 Total rows and columns
  - ⚠️ Missing values
  - 🔄 Duplicate records
  - 🔍 Issues found (with severity)
- **Provide Feedback**: Click 👍 if satisfied, 👎 if not
  - If 👎: Agent will ask for comments and re-assess

#### **Step 4: Select Report Format**
- Choose from: PDF, Excel, JSON, HTML, CSV
- Report will be generated in selected format

#### **Step 5: ETL Code Generation**
- Select programming language:
  - 🐍 **Python/Pandas** - Best for data science workflows
  - ⚡ **PySpark** - Best for big data/distributed processing
  - 📘 **Node.js** - Best for JavaScript/TypeScript projects
- Review generated code
- **Actions**:
  - 📋 Copy code to clipboard
  - 💾 Download as file
- **Provide Feedback**: Click 👍 if code is good, 👎 to regenerate

#### **Step 6: Work on Duplicates (Optional)**
After ETL code generation, you'll see:
- **"Would you like to work on duplicate data?"**
  - ✅ **Yes, Clean My Data** → Go to Step 7
  - ❌ **No, Code is Enough** → Workflow complete, use the code

#### **Step 7: Confirm Cleaning (if selected)**
- Review what will be done:
  - Remove duplicate records
  - Handle missing values
  - Standardize data types
  - Save to blob storage
- **Confirmation Required**: "Are you sure?"
  - ✅ **Yes, Proceed** → Start cleaning
  - ❌ **Cancel** → Return to previous step

#### **Step 8: Data Cleaning Process**
- Watch real-time progress
- AI learns from previous feedback (if any)
- Operations performed:
  - 🧹 Duplicate removal
  - 📝 Missing value imputation
  - 🔄 Data type standardization
  - ✅ Validation

#### **Step 9: View Results**
For each cleaned file, see:
- Original row count
- Cleaned row count
- Duplicates removed
- Missing values handled
- **Blob Storage URL** (copy or download)

**Actions**:
- 📥 Download all cleaned files
- 📋 Copy blob URLs
- 👍/👎 Provide final feedback

#### **Step 10: Complete**
- Click "Complete Pipeline" to finish
- Or start a new pipeline with different data

---

## 💡 Tips

### Feedback System
- **Always provide feedback** when outputs aren't satisfactory
- Be specific in comments for better improvements
- The AI learns and adapts based on your preferences

### File Selection
- You can select **multiple files** at once
- All selected files will be processed together
- Each file gets individual assessment and cleaning results

### Code Usage
- **Copy** the generated ETL code
- **Customize** connection strings and credentials
- **Test** in your environment before production use
- Code includes error handling and logging

### Data Cleaning
- **Original data is preserved** - cleaning creates new versions
- Cleaned files are saved with timestamps
- You can always re-run cleaning with different settings

---

## 🎯 Best Practices

1. **Start Small**: Test with 1-2 files first
2. **Review Assessments**: Always check the quality report before proceeding
3. **Provide Feedback**: Help the AI learn your preferences
4. **Save Blob URLs**: Keep track of cleaned file locations
5. **Version Control**: Download code for version tracking

---

## 🔧 Troubleshooting

### Issue: "No databases found"
- Check database connections in settings
- Verify credentials

### Issue: "Assessment taking too long"
- Large files may take more time
- Wait for progress to complete

### Issue: "Can't download cleaned files"
- Check blob storage configuration
- Verify network connection
- Copy URL manually

### Issue: "ETL code doesn't work"
- Update connection strings
- Install required dependencies
- Check database credentials

---

## 📞 Need Help?

- Check the full documentation: `DATA_PIPELINE_README.md`
- Report issues in the repository
- Contact support team

---

**Happy Data Processing! 🎉**
