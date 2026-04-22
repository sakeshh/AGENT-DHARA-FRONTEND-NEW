# Data Pipeline Workflow - Complete ETL System

## Overview

This is a comprehensive AI-powered data analysis and ETL (Extract, Transform, Load) workflow application built with Next.js 14, React, TypeScript, and Tailwind CSS. The system provides an intelligent, user-feedback-driven approach to data cleaning, assessment, and ETL code generation.

## Features

### 🗄️ **Database Selection**
- View all available databases from multiple sources
- Support for PostgreSQL, MySQL, MongoDB, SQL Server, and Oracle
- Search and filter databases by name and type
- Visual database cards with type-specific icons

### 📁 **File Selection**
- Browse all tables/collections within selected database
- Select single or multiple files for processing
- View file metadata (size, row count, type)
- Real-time search and filtering

### 📊 **Data Assessment**
- Automated data quality analysis
- Detection of:
  - Missing values
  - Duplicate records
  - Data type inconsistencies
  - Column statistics
- Quality scoring (0-100%)
- Detailed issue reporting with severity levels
- Real-time progress tracking

### 📈 **Report Generation**
- Multiple export formats:
  - PDF
  - Excel (XLSX)
  - JSON
  - HTML
  - CSV
- Comprehensive data quality metrics
- Visual quality indicators

### 💻 **ETL Code Generation**
- Support for multiple languages/frameworks:
  - **Python/Pandas** - For data science workflows
  - **PySpark** - For big data processing
  - **Node.js** - For JavaScript/TypeScript environments
- Auto-generated production-ready code
- Features:
  - Data extraction from databases
  - Duplicate removal
  - Missing value handling
  - Data type standardization
  - Quality report generation
- Copy to clipboard or download code

### 🧹 **Data Cleaning**
- Confirmation workflow for safety
- Automated cleaning process:
  - Duplicate record removal
  - Missing value imputation (median for numeric, mode for categorical)
  - Data type standardization
  - Data validation
- Real-time progress tracking
- Blob storage integration for cleaned files

### 🎯 **AI Learning System**
- User feedback collection at each step
- Like/Dislike mechanism for continuous improvement
- Re-generation based on feedback
- Learning from user preferences and corrections
- Adaptive processing strategies

### 📦 **Blob Storage Integration**
- Cleaned files saved to Azure Blob Storage (or compatible)
- Unique timestamped file names
- Direct download links
- Easy file sharing and archiving

## User Workflow

### Step-by-Step Process:

1. **Database Selection**
   - User views all available databases
   - Selects target database
   - System loads available tables/files

2. **File Selection**
   - User browses available files
   - Selects one or multiple files
   - Proceeds to assessment

3. **Data Assessment**
   - Agent automatically analyzes data quality
   - Shows comprehensive report with:
     - Total rows and columns
     - Missing values count
     - Duplicate records count
     - Quality score
     - Detailed issues list
   - User can provide feedback (like/dislike)
   - If disliked, agent re-assesses with improvements

4. **Report Format Selection**
   - User approves assessment
   - Selects desired report format (PDF, Excel, JSON, HTML, CSV)
   - System generates formatted report

5. **ETL Code Generation**
   - Agent generates ETL code automatically
   - User selects preferred language (Python/Pandas, PySpark, Node.js)
   - Code includes:
     - Data extraction logic
     - Transformation and cleaning steps
     - Loading to destination
     - Quality reporting
   - User can copy or download code
   - User provides feedback (like/dislike)
   - If disliked, agent regenerates improved code

6. **Duplicate Data Handling Decision**
   - Agent asks: "Do you want to work on duplicate data?"
   - Options:
     - **Yes, Clean My Data** → Proceeds to cleaning step
     - **No, Code is Enough** → Completes workflow with code only

7. **Confirmation (if cleaning selected)**
   - Agent shows confirmation dialog
   - Lists all operations that will be performed
   - User must confirm: "Are you sure?"
   - Options:
     - **Yes, Proceed with Cleaning**
     - **Cancel**

8. **Data Cleaning Process**
   - Agent performs cleaning with real-time progress
   - Operations:
     - Remove duplicates
     - Fill missing values
     - Standardize data types
     - Validate data integrity
   - Saves cleaned files to blob storage
   - Shows results with before/after metrics

9. **Results & Download**
   - Displays cleaning results for each file:
     - Original row count
     - Cleaned row count
     - Duplicates removed
     - Missing values handled
     - Blob storage URL
   - User can:
     - Copy blob URLs
     - Download all cleaned files
     - Provide feedback (like/dislike)
   - User confirms completion

10. **Feedback Learning**
    - Throughout the process, if user dislikes any output:
      - Agent prompts for specific feedback/comment
      - Agent learns from the feedback
      - Agent reworks and improves the output
      - Process repeats until user is satisfied

## Technology Stack

- **Framework**: Next.js 14 (App Router)
- **UI Library**: React 18
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **Animations**: Framer Motion
- **Icons**: React Icons, Simple Icons
- **State Management**: React Hooks (useState, useEffect)

## Installation

```bash
# Clone the repository
git clone <repository-url>

# Navigate to project directory
cd my-app-sakesh2

# Install dependencies
npm install

# Run development server
npm run dev
```

## Usage

1. Start the development server:
```bash
npm run dev
```

2. Navigate to `http://localhost:3000`

3. Login/Sign up at `/auth`

4. Access the Data Pipeline from the sidebar in `/chat` or directly at `/data-pipeline`

5. Follow the guided workflow:
   - Select database
   - Choose files
   - Review assessment
   - Select report format
   - Generate ETL code
   - Choose to clean data (optional)
   - Download cleaned files

## Project Structure

```
my-app-sakesh2/
├── app/
│   ├── auth/
│   │   └── page.tsx                 # Authentication page
│   ├── chat/
│   │   └── page.tsx                 # Chat interface
│   ├── data-pipeline/
│   │   └── page.tsx                 # Main pipeline workflow
│   ├── layout.tsx                   # Root layout
│   ├── page.tsx                     # Landing page
│   └── globals.css                  # Global styles
├── components/
│   ├── ChatWindow.tsx               # Chat interface component
│   ├── DataAssessment.tsx           # Sidebar data assessment section
│   ├── DataAssessmentReport.tsx     # Assessment report display
│   ├── DataCleaner.tsx              # Data cleaning component
│   ├── DataOrchestration.tsx        # Orchestration section
│   ├── DataQuality.tsx              # Quality metrics section
│   ├── DataTransformation.tsx       # Transformation section
│   ├── DatabaseSelector.tsx         # Database selection UI
│   ├── ETLCodeGenerator.tsx         # ETL code generation
│   ├── FileSelector.tsx             # File selection UI
│   ├── Monitoring.tsx               # Monitoring section
│   └── Sidebar.tsx                  # Main sidebar
├── public/                          # Static assets
├── types/
│   └── index.ts                     # TypeScript type definitions
├── package.json                     # Dependencies
├── tsconfig.json                    # TypeScript configuration
├── tailwind.config.ts               # Tailwind configuration
└── next.config.ts                   # Next.js configuration
```

## Key Components

### DatabaseSelector
Displays available databases with search functionality and type-specific icons.

### FileSelector
Shows files/tables within selected database with metadata and multi-select capability.

### DataAssessmentReport
Analyzes data quality and displays comprehensive metrics with feedback mechanism.

### ETLCodeGenerator
Generates production-ready ETL code in multiple languages with copy/download functionality.

### DataCleaner
Handles data cleaning workflow with confirmation, progress tracking, and blob storage integration.

## AI Learning Features

The application learns from user feedback at each step:

1. **Feedback Collection**: Like/Dislike buttons at each major step
2. **Comment Gathering**: When user dislikes, system asks for specific feedback
3. **Adaptive Processing**: Agent adjusts processing based on feedback
4. **Re-generation**: Automatically improves outputs based on learned preferences
5. **Context Awareness**: Remembers user preferences throughout the session

## Configuration

### Database Connections
Edit `components/DatabaseSelector.tsx` to add/modify database connections:

```typescript
const mockDatabases = [
  { id: 'postgres-prod', name: 'PostgreSQL Production', type: 'postgresql', ... },
  // Add your databases here
];
```

### Blob Storage
Configure blob storage in `components/DataCleaner.tsx`:

```typescript
blobUrl: `https://your-storage-account.blob.core.windows.net/container/${fileName}`
```

## Future Enhancements

- [ ] Real database connectivity (currently using mock data)
- [ ] Azure Blob Storage integration
- [ ] Real-time data streaming
- [ ] Advanced ML-based data quality detection
- [ ] Automated scheduling and orchestration
- [ ] Data lineage tracking
- [ ] Version control for cleaned datasets
- [ ] Collaborative features for team workflows
- [ ] API endpoints for programmatic access
- [ ] Custom ETL template creation

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.

## Support

For issues, questions, or feedback, please open an issue in the repository.

---

**Built with ❤️ using Next.js and AI-powered workflows**
