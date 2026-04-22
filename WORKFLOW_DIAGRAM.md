# Data Pipeline Workflow - Visual Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE WORKFLOW                            │
└─────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────┐
                              │    START     │
                              └──────┬───────┘
                                     │
                                     ▼
                         ┌─────────────────────┐
                         │  1. Select Database │
                         │  - View all DBs     │
                         │  - Search/Filter    │
                         │  - Click to select  │
                         └─────────┬───────────┘
                                   │
                                   ▼
                         ┌─────────────────────┐
                         │  2. Choose Files    │
                         │  - Browse tables    │
                         │  - Single/Multi     │
                         │  - View metadata    │
                         └─────────┬───────────┘
                                   │
                                   ▼
                         ┌─────────────────────────┐
                         │  3. Data Assessment     │
                         │  - Auto analyze         │
                         │  - Quality score        │
                         │  - Issues detected      │
                         └─────────┬───────────────┘
                                   │
                                   ▼
                         ┌─────────────────────┐
                         │   👍 Satisfied?     │
                         └─────┬───────┬───────┘
                               │       │
                          Yes  │       │  No
                               │       │
                               │       ▼
                               │  ┌──────────────────┐
                               │  │ Provide Feedback │
                               │  │ Agent Re-assess  │
                               │  └────────┬─────────┘
                               │           │
                               │           └──────┐
                               ▼                  │
                    ┌─────────────────────┐      │
                    │ 4. Select Report    │◄─────┘
                    │    Format           │
                    │ - PDF, Excel, JSON  │
                    │ - HTML, CSV         │
                    └─────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────────┐
                    │ 5. Generate ETL Code    │
                    │ - Python/Pandas         │
                    │ - PySpark               │
                    │ - Node.js               │
                    └─────────┬───────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   👍 Code Good?     │
                    └─────┬───────┬───────┘
                          │       │
                     Yes  │       │  No
                          │       │
                          │       ▼
                          │  ┌──────────────────┐
                          │  │ Provide Feedback │
                          │  │ Agent Regenerate │
                          │  └────────┬─────────┘
                          │           │
                          │           └──────┐
                          ▼                  │
              ┌─────────────────────────┐   │
              │ 6. Work on Duplicates?  │◄──┘
              └─────┬────────────┬──────┘
                    │            │
          Yes       │            │  No (Code Enough)
                    │            │
                    │            ▼
                    │    ┌──────────────┐
                    │    │   COMPLETE   │
                    │    │ Download Code│
                    │    └──────────────┘
                    │
                    ▼
        ┌─────────────────────────┐
        │ 7. Confirmation Dialog  │
        │ "Are you sure?"         │
        └─────┬────────────┬──────┘
              │            │
         Yes  │            │  Cancel
              │            │
              │            └────────┐
              ▼                     │
    ┌─────────────────────────┐    │
    │ 8. Cleaning Process     │    │
    │ - Remove duplicates     │    │
    │ - Handle missing values │    │
    │ - Standardize types     │    │
    │ - AI learns from        │    │
    │   previous feedback     │    │
    └─────────┬───────────────┘    │
              │                     │
              ▼                     │
    ┌─────────────────────────┐    │
    │ 9. View Results         │    │
    │ - Original/Clean counts │    │
    │ - Blob storage URLs     │    │
    │ - Download options      │    │
    └─────────┬───────────────┘    │
              │                     │
              ▼                     │
    ┌─────────────────────┐        │
    │   👍 Satisfied?     │        │
    └─────┬───────┬───────┘        │
          │       │                 │
     Yes  │       │  No            │
          │       │                 │
          │       ▼                 │
          │  ┌──────────────────┐  │
          │  │ Provide Feedback │  │
          │  │ Agent Rework     │  │
          │  └────────┬─────────┘  │
          │           │             │
          │           └──────┐      │
          ▼                  │      │
    ┌──────────┐            │      │
    │ COMPLETE │◄───────────┴──────┘
    │ Download │
    │  Files   │
    └────┬─────┘
         │
         ▼
    ┌──────────┐
    │   END    │
    └──────────┘


┌─────────────────────────────────────────────────────────────────────────┐
│                         FEEDBACK LEARNING LOOP                           │
└─────────────────────────────────────────────────────────────────────────┘

                    ┌────────────────────────┐
                    │  User Action/Output    │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │  User Provides         │
                    │  Feedback (👍/👎)      │
                    └───────────┬────────────┘
                                │
                  ┌─────────────┴─────────────┐
                  │                           │
                  ▼                           ▼
        ┌──────────────────┐      ┌──────────────────┐
        │   👍 Liked       │      │   👎 Disliked    │
        │  - Save to       │      │  - Request       │
        │    preference    │      │    comment       │
        │  - Continue      │      │  - Analyze       │
        └──────────────────┘      └────────┬─────────┘
                                            │
                                            ▼
                                  ┌──────────────────┐
                                  │  AI Learns       │
                                  │  - Store feedback│
                                  │  - Adjust params │
                                  │  - Rework output │
                                  └────────┬─────────┘
                                           │
                                           ▼
                                  ┌──────────────────┐
                                  │  Present Improved│
                                  │  Output          │
                                  └────────┬─────────┘
                                           │
                                           └───────┐
                                                   │
                    ┌──────────────────────────────┘
                    │
                    ▼
        ┌────────────────────────┐
        │  User Reviews Again    │
        └───────────┬────────────┘
                    │
                    └─────► (Feedback Loop Repeats)


┌─────────────────────────────────────────────────────────────────────────┐
│                         KEY DECISION POINTS                              │
└─────────────────────────────────────────────────────────────────────────┘

1. Assessment Satisfaction → Determines if re-assessment needed
2. Code Quality → Determines if regeneration needed
3. Work on Duplicates → Branches to cleaning or completion
4. Confirmation → Safety check before cleaning
5. Final Satisfaction → Determines if rework needed

┌─────────────────────────────────────────────────────────────────────────┐
│                         OUTPUT ARTIFACTS                                 │
└─────────────────────────────────────────────────────────────────────────┘

📊 Data Assessment Report
   └─► Quality metrics, issues, statistics

💻 ETL Code
   └─► Production-ready code in selected language

🧹 Cleaned Data Files
   └─► Stored in blob storage with URLs

📈 Quality Reports
   └─► Exported in selected format (PDF/Excel/etc)

🔗 Blob Storage URLs
   └─► Direct links to cleaned files


┌─────────────────────────────────────────────────────────────────────────┐
│                         PROGRESS TRACKING                                │
└─────────────────────────────────────────────────────────────────────────┘

[Database] → [Files] → [Assessment] → [Report] → [ETL] → [Cleaning]
    ✓           ✓           ✓            ✓         ✓         ⏳

Each step shows visual progress indicators and can be revisited
with feedback mechanism for improvements.
```

## Color-Coded Severity in Assessment

```
🔴 High Severity   → Critical issues requiring immediate attention
🟡 Medium Severity → Important issues to address
🔵 Low Severity    → Minor issues, optional fixes
```

## Quality Score Indicators

```
90-100% → 🟢 Excellent (Green)
70-89%  → 🟡 Good (Yellow)
0-69%   → 🔴 Needs Improvement (Red)
```

## File Processing States

```
⏳ Pending    → Waiting to be processed
🔄 Processing → Currently being cleaned
✅ Complete   → Successfully processed
❌ Failed     → Error occurred (with reason)
```
