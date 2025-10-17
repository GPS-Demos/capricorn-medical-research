# Capricorn Medical Research Application

A medical research application that uses AI to analyze pediatric oncology cases and provide treatment recommendations by searching through PubMed articles using vector embeddings and Gemini.

## Architecture Overview

![Architecture Diagram](visuals/capricorn_architecture.png)

The application consists of:
- **Frontend**: React application hosted on Firebase
- **Backend**: Cloud Functions for data processing pipeline
- **Database**: Firestore for chat storage, BigQuery for article embeddings
- **AI Services**: Gemini for analysis, DLP for PII redaction

## Prerequisites

- Google Cloud Platform account with billing enabled
- Node.js 18 or higher
- Firebase CLI installed (`npm install -g firebase-tools`)
- gcloud CLI installed and configured
- SendGrid account for email notifications

## Setup Instructions

### 1. Firestore Database Setup

Create a Firestore database for storing chat conversations.

**Prerequisites:**
- Install gcloud CLI: https://cloud.google.com/sdk/docs/install
- Authenticate: `gcloud auth login`
- Set project: `gcloud config set project YOUR_GCP_PROJECT_ID`

**1. Set project variables and enable APIs:**
```bash
# Set your configuration values
export PROJECT_ID="YOUR_GCP_PROJECT_ID"
export DATABASE_ID="YOUR_DATABASE_ID"  # e.g., "capricorn-prod", "capricorn-dev"
export DATABASE_LOCATION="YOUR_LOCATION"  # See locations below

# Enable required APIs
gcloud services enable \
  firestore.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  dlp.googleapis.com \
  aiplatform.googleapis.com \
  bigquery.googleapis.com \
  --project=$PROJECT_ID
```

**2. Create the database:**
```bash
# Create the Firestore database
gcloud firestore databases create \
  --database=$DATABASE_ID \
  --location=$DATABASE_LOCATION \
  --project=$PROJECT_ID
```

**Choose a location:**
See available locations at: https://firebase.google.com/docs/firestore/locations
- Multi-region options: `nam5` (US), `eur3` (Europe)
- Regional options: `us-central1`, `europe-west1`, etc.

**2. After creating the database, update the frontend configuration:**
```bash
cd frontend
cp .env.example .env
# Edit .env and set:
# REACT_APP_FIREBASE_DATABASE_ID=your-database-id
```

**Note**: Backend configuration will be handled in the next section using the setup script.

**3. Create Firestore security rules:**
```javascript
// Create a file named firestore.rules in your project root
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    // Allow authenticated users to read/write their own chats
    match /chats/{userId}/conversations/{document=**} {
      allow read, write: if request.auth != null && request.auth.uid == userId;
    }
  }
}
```

**4. Deploy the security rules:**
```bash
firebase deploy --only firestore:rules
```

### 2. Cloud Functions Setup

Each Cloud Function needs to be deployed with the appropriate environment variables:

#### 2.1 Create Backend Configuration

Use the provided setup script to create all `.env.yaml` files automatically:

```bash
# Run the setup script
./setup-backend-config.sh
```

The script will prompt you for:
- GCP Project ID
- Firestore Database ID
- Cloud Functions region
- BigQuery Project ID (optional, defaults to GCP Project ID)
- PMID dataset name (e.g., pubmed)
- Journal dataset name (e.g., journal)
- SendGrid API key (optional)

This creates `.env.yaml` files for all Cloud Functions:
- `backend/capricorn-chat/.env.yaml`
- `backend/capricorn-redact-sensitive-info/.env.yaml`
- `backend/capricorn-process-lab/.env.yaml`
- `backend/capricorn-retrieve-full-articles/.env.yaml`
- `backend/capricorn-final-analysis/.env.yaml`
- `backend/capricorn-feedback/.env.yaml`

**Note**: The `.env.yaml` files are already in `.gitignore` to prevent committing sensitive data.

**Environment Variables Reference:**

Each Cloud Function uses specific environment variables configured in its `.env.yaml` file:

- **Common variables across all functions:**
  - `GCP_PROJECT`: Your Google Cloud Project ID
  - `FIRESTORE_DATABASE_ID`: The Firestore database ID (e.g., "capricorn-prod")
  - `CLOUD_FUNCTIONS_REGION`: The region where functions are deployed

- **BigQuery-specific variables:**
  - `BIGQUERY_PROJECT_ID`: Project ID containing BigQuery datasets (defaults to GCP_PROJECT)
  - `PMID_DATASET`: Dataset name for PubMed articles (e.g., "pubmed")
  - `JOURNAL_DATASET`: Dataset name for journal rankings (e.g., "journal_rank")

- **Function-specific variables:**
  - `SENDGRID_API_KEY`: (capricorn-feedback only) API key for sending feedback emails
  - Additional model-specific configurations for Gemini AI

The setup script (`setup-backend-config.sh`) automatically creates these files with the correct values based on your input.

#### 2.2 Deploy Cloud Functions

Deploy each function with its configuration:

```bash
# Deploy all functions
cd backend

# Redact Sensitive Info
cd capricorn-redact-sensitive-info
gcloud functions deploy redact-sensitive-info \
  --gen2 \
  --runtime=python312 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=redact_sensitive_info \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file=.env.yaml

# Process Lab
cd ../capricorn-process-lab
gcloud functions deploy process-lab \
  --gen2 \
  --runtime=python313 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=process_lab \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=4 \
  --memory=4Gi \
  --timeout=3600s \
  --max-instances=100 \
  --min-instances=1 \
  --concurrency=80 \
  --env-vars-file=.env.yaml

# Retrieve Full Articles
cd ../capricorn-retrieve-full-articles
gcloud functions deploy retrieve-full-articles-live-pmc-text-embeddings-005 \
  --gen2 \
  --runtime=python312 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=retrieve_full_articles \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=6 \
  --memory=8Gi \
  --timeout=3600s \
  --max-instances=100 \
  --concurrency=1 \
  --env-vars-file=.env.yaml

# Final Analysis
cd ../capricorn-final-analysis
gcloud functions deploy final-analysis \
  --gen2 \
  --runtime=python312 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=final_analysis \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=6 \
  --memory=8Gi \
  --timeout=3600s \
  --max-instances=100 \
  --concurrency=1 \
  --env-vars-file=.env.yaml

# Chat
cd ../capricorn-chat
gcloud functions deploy chat \
  --gen2 \
  --runtime=python312 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=chat \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=8 \
  --memory=8Gi \
  --timeout=3600s \
  --max-instances=100 \
  --concurrency=1 \
  --env-vars-file=.env.yaml

# Feedback
cd ../capricorn-feedback
gcloud functions deploy send-feedback-email \
  --gen2 \
  --runtime=python39 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=send_feedback_email \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=1 \
  --memory=512Mi \
  --timeout=300s \
  --max-instances=100 \
  --concurrency=80 \
  --env-vars-file=.env.yaml

# Extract Disease
cd ../pubmed-search-tester-extract-disease
gcloud functions deploy extract-disease \
  --gen2 \
  --runtime=python312 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=extract_disease \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=2 \
  --memory=1Gi \
  --timeout=600s \
  --max-instances=100 \
  --concurrency=1 \
  --env-vars-file=.env.yaml

# Extract Events
cd ../pubmed-search-tester-extract-events
gcloud functions deploy extract-events \
  --gen2 \
  --runtime=python312 \
  --region=YOUR_REGION \
  --source=. \
  --entry-point=extract_events \
  --trigger-http \
  --allow-unauthenticated \
  --cpu=2 \
  --memory=1Gi \
  --timeout=600s \
  --max-instances=100 \
  --concurrency=1 \
  --env-vars-file=.env.yaml
```

#### 2.3 Collect Function URLs and Update Frontend

After deploying all functions, collect their URLs and automatically update the frontend:

```bash
# Navigate back to project root
cd ../..

# Set your region (must match deployment region from section 2.2)
export REGION=YOUR_REGION

# Collect all function URLs
echo "Collecting Cloud Function URLs..."
REDACT_URL=$(gcloud functions describe redact-sensitive-info --region=$REGION --format='value(serviceConfig.uri)')
PROCESS_LAB_URL=$(gcloud functions describe process-lab --region=$REGION --format='value(serviceConfig.uri)')
RETRIEVE_ARTICLES_URL=$(gcloud functions describe retrieve-full-articles-live-pmc-text-embeddings-005 --region=$REGION --format='value(serviceConfig.uri)')
FINAL_ANALYSIS_URL=$(gcloud functions describe final-analysis --region=$REGION --format='value(serviceConfig.uri)')
CHAT_URL=$(gcloud functions describe chat --region=$REGION --format='value(serviceConfig.uri)')
FEEDBACK_URL=$(gcloud functions describe send-feedback-email --region=$REGION --format='value(serviceConfig.uri)')
EXTRACT_DISEASE_URL=$(gcloud functions describe extract-disease --region=$REGION --format='value(serviceConfig.uri)')
EXTRACT_EVENTS_URL=$(gcloud functions describe extract-events --region=$REGION --format='value(serviceConfig.uri)')

# Save URLs to file for reference
{
  echo "# Cloud Function URLs - Generated $(date)"
  echo "REDACT_URL=$REDACT_URL"
  echo "PROCESS_LAB_URL=$PROCESS_LAB_URL"
  echo "RETRIEVE_ARTICLES_URL=$RETRIEVE_ARTICLES_URL"
  echo "FINAL_ANALYSIS_URL=$FINAL_ANALYSIS_URL"
  echo "CHAT_URL=$CHAT_URL"
  echo "FEEDBACK_URL=$FEEDBACK_URL"
  echo "EXTRACT_DISEASE_URL=$EXTRACT_DISEASE_URL"
  echo "EXTRACT_EVENTS_URL=$EXTRACT_EVENTS_URL"
} > function-urls.txt

# Update api.js with the correct URLs
# Note: The API_BASE_URL is used for multiple functions, so we'll use the Chat function's base URL
API_BASE_URL=$(echo $CHAT_URL | sed 's|/chat$||')

# Update the hardcoded URLs in api.js
sed -i.bak "s|const API_BASE_URL = .*|const API_BASE_URL = '$API_BASE_URL';|" frontend/src/utils/api.js
sed -i.bak "s|https://capricorn-feedback-[^']*|$FEEDBACK_URL|" frontend/src/utils/api.js
sed -i.bak "s|https://capricorn-process-lab-[^']*|$PROCESS_LAB_URL|" frontend/src/utils/api.js

echo "✓ Updated frontend/src/utils/api.js with Cloud Function URLs"
echo "✓ Saved URLs to function-urls.txt for reference"
```

### 3. BigQuery Setup

The application requires BigQuery datasets with PubMed article embeddings and metadata.

#### 3.1 Journal Impact Factor Dataset Setup

The application uses journal impact factor data (SJR scores) to rank articles. The repository includes a pre-downloaded SCImago Journal Rank CSV file.

**Prerequisites:**
- Ensure you have a Python virtual environment set up
- Ensure you have the environment variables from section 1 still set

**Load the journal data into BigQuery:**

```bash
# Create and/or activate Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required dependencies
pip install google-cloud-bigquery

# Navigate to the function directory and run the loading script
cd backend/capricorn-retrieve-full-articles
python load_journal_data_to_bq.py \
  --project-id $BIGQUERY_PROJECT_ID \
  --dataset-id journal_rank \
  --table-id scimagojr_2024 \
  --csv-file scimagojr_2024.csv
```

This script will:
- Create a `journal_rank` dataset if it doesn't exist
- Load the SCImago journal data into a table named `scimagojr_2024`
- Display sample data and confirm successful loading

**Verify the data loaded correctly:**
```bash
# Check the record count (should show ~29,000+ journals)
bq query --use_legacy_sql=false \
"SELECT COUNT(*) as journal_count FROM \`$BIGQUERY_PROJECT_ID.journal_rank.scimagojr_2024\`"
```

#### 3.2 Service Account Setup

1. Create a service account:
   ```bash
   gcloud iam service-accounts create capricorn-bigquery-reader \
     --display-name="Capricorn BigQuery Reader" \
     --project=$PROJECT_ID
   ```

2. Grant required permissions:
   ```bash
   # Grant BigQuery permissions
   gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT_ID \
     --member="serviceAccount:capricorn-bigquery-reader@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer"
   
   # Grant Vertex AI permissions (required for Gemini embeddings model access)
   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:capricorn-bigquery-reader@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/aiplatform.user"
   ```

3. Create and download service account key:
   ```bash
   gcloud iam service-accounts keys create bigquery-service-account.json \
     --iam-account=capricorn-bigquery-reader@$PROJECT_ID.iam.gserviceaccount.com
   ```

#### 3.2 BigQuery Dataset Transfer

**Important**: To set up the BigQuery dataset with PubMed embeddings, you need assistance from the Google team:

1. Contact the GPS-RIT team at: **gps-rit@google.com**
2. Follow the setup guide: https://docs.google.com/document/d/1__5TOLrIEoUbiksQdNVKUR5hQW5E0o9nOM-WBiHyzjo/edit?tab=t.0

### 4. Frontend Configuration

#### 4.1 Get Firebase Configuration Values

1. **Access Firebase Console**:
   - Go to [Firebase Console](https://console.firebase.google.com)
   - Create a new project or select your existing GCP project
   - If creating new, link it to your GCP project when prompted

2. **Find Your Configuration**:
   - Click the gear icon ⚙️ → "Project settings"
   - Scroll down to "Your apps" section
   - If no app exists, click "Add app" → Choose Web (</>)
   - Register your app with a nickname (e.g., "capricorn-medical")
   - You'll see your Firebase configuration:
   ```javascript
   const firebaseConfig = {
     apiKey: "...",              // → REACT_APP_FIREBASE_API_KEY
     authDomain: "...",          // → REACT_APP_FIREBASE_AUTH_DOMAIN
     projectId: "...",           // → REACT_APP_FIREBASE_PROJECT_ID
     storageBucket: "...",       // → REACT_APP_FIREBASE_STORAGE_BUCKET
     messagingSenderId: "...",   // → REACT_APP_FIREBASE_MESSAGING_SENDER_ID
     appId: "..."                // → REACT_APP_FIREBASE_APP_ID
   };
   ```

#### 4.2 Create Environment Configuration

```bash
cd frontend

# Create .env file (already in .gitignore)
cat > .env << EOF
REACT_APP_FIREBASE_API_KEY=YOUR_FIREBASE_API_KEY
REACT_APP_FIREBASE_AUTH_DOMAIN=YOUR_PROJECT_ID.firebaseapp.com
REACT_APP_FIREBASE_PROJECT_ID=YOUR_PROJECT_ID
REACT_APP_FIREBASE_STORAGE_BUCKET=YOUR_PROJECT_ID.appspot.com
REACT_APP_FIREBASE_MESSAGING_SENDER_ID=YOUR_MESSAGING_SENDER_ID
REACT_APP_FIREBASE_APP_ID=YOUR_APP_ID
REACT_APP_FIREBASE_DATABASE_ID=YOUR_DATABASE_ID
EOF
```

Replace the placeholders with your actual values from the Firebase configuration. Use the `DATABASE_ID` you created in section 1 (e.g., "capricorn-prod")

#### 4.3 Update Firebase Hosting Configuration

Update `firebase.json` with your project ID:
```bash
# Get the project ID from the .env file
PROJECT_ID=$(grep REACT_APP_FIREBASE_PROJECT_ID .env | cut -d '=' -f2)

# Update firebase.json with the project ID
sed -i.bak "s|\"site\": \".*\"|\"site\": \"$PROJECT_ID\"|" firebase.json

echo "✓ Updated firebase.json with site name: $PROJECT_ID"
```

### 5. Deploy Frontend

1. Build the application:
   ```bash
   cd frontend
   npm install
   npm run build
   ```

2. Deploy to Firebase Hosting:
   ```bash
   # Deploy using the existing firebase.json configuration
   firebase deploy --only hosting --project $PROJECT_ID
   ```
