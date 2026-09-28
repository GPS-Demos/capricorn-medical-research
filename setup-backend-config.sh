#!/bin/bash

# Setup script for Capricorn Medical Research backend configuration
# This script creates .env.yaml files for all Cloud Functions

set -euo pipefail
umask 077

echo "=================================================="
echo "Capricorn Medical Research - Backend Configuration"
echo "=================================================="
echo ""
echo "This script will create .env.yaml files for all Cloud Functions."
echo "You'll be prompted for common values once."
echo ""

validate_input() {
    local val="$1"
    local name="$2"
    if [[ "$val" =~ [\$\`\"\\\n\r] ]]; then
        echo "Error: Invalid character in $name" >&2
        exit 1
    fi
}

# Prompt for common values
read -p "Enter your GCP Project ID: " PROJECT_ID
validate_input "$PROJECT_ID" "Project ID"

read -p "Enter your Firestore Database ID (e.g., capricorn-prod): " DATABASE_ID
validate_input "$DATABASE_ID" "Database ID"

read -p "Enter your Cloud Functions region (e.g., us-central1): " REGION
validate_input "$REGION" "Region"

read -p "Enter your BigQuery Project ID (press Enter if same as GCP Project): " BIGQUERY_PROJECT_ID
if [ -n "$BIGQUERY_PROJECT_ID" ]; then
    validate_input "$BIGQUERY_PROJECT_ID" "BigQuery Project ID"
fi

# Use GCP Project ID if BigQuery Project ID is empty
if [ -z "$BIGQUERY_PROJECT_ID" ]; then
    BIGQUERY_PROJECT_ID=$PROJECT_ID
fi

# Prompt for BigQuery dataset names
read -p "Enter your PMID dataset name (e.g., pmid_uscentral): " PMID_DATASET
validate_input "$PMID_DATASET" "PMID Dataset"

read -p "Enter your Journal dataset name (e.g., journal_rank): " JOURNAL_DATASET
validate_input "$JOURNAL_DATASET" "Journal Dataset"

# Optional: Prompt for SendGrid API key
echo ""
echo "SendGrid API key is required for the feedback function."
echo "You can enter it now or add it manually later to backend/capricorn-feedback/.env.yaml"
read -p "Enter your SendGrid API key (optional, press Enter to skip): " SENDGRID_API_KEY
if [ -n "$SENDGRID_API_KEY" ]; then
    validate_input "$SENDGRID_API_KEY" "SendGrid API Key"
fi

echo ""
echo "Creating configuration files..."

# Create capricorn-chat/.env.yaml
cat > backend/capricorn-chat/.env.yaml << EOF
# Environment variables for capricorn-chat Cloud Function
PROJECT_ID: "$PROJECT_ID"
DATABASE_ID: "$DATABASE_ID"
LOCATION: "$REGION"
EOF
echo "✓ Created backend/capricorn-chat/.env.yaml"

# Create capricorn-redact-sensitive-info/.env.yaml
cat > backend/capricorn-redact-sensitive-info/.env.yaml << EOF
# Environment variables for capricorn-redact-sensitive-info Cloud Function
PROJECT_ID: "$PROJECT_ID"
DLP_PROJECT_ID: "$PROJECT_ID"  # Using same as PROJECT_ID
LOCATION: "$REGION"
EOF
echo "✓ Created backend/capricorn-redact-sensitive-info/.env.yaml"

# Create capricorn-process-lab/.env.yaml
cat > backend/capricorn-process-lab/.env.yaml << EOF
# Environment variables for capricorn-process-lab Cloud Function
PROJECT_ID: "$PROJECT_ID"
LOCATION: "global"  # This function uses global location for document processing
EOF
echo "✓ Created backend/capricorn-process-lab/.env.yaml"

# Create capricorn-retrieve-full-articles/.env.yaml
cat > backend/capricorn-retrieve-full-articles/.env.yaml << EOF
# Environment variables for capricorn-retrieve-full-articles Cloud Function
GENAI_PROJECT_ID: "$PROJECT_ID"
BIGQUERY_PROJECT_ID: "$BIGQUERY_PROJECT_ID"
LOCATION: "$REGION"
PMID_DATASET: "$PMID_DATASET"
JOURNAL_DATASET: "$JOURNAL_DATASET"
EOF
echo "✓ Created backend/capricorn-retrieve-full-articles/.env.yaml"

# Create capricorn-final-analysis/.env.yaml
cat > backend/capricorn-final-analysis/.env.yaml << EOF
# Environment variables for capricorn-final-analysis Cloud Function
GENAI_PROJECT_ID: "$PROJECT_ID"
BIGQUERY_PROJECT_ID: "$BIGQUERY_PROJECT_ID"
LOCATION: "$REGION"
EOF
echo "✓ Created backend/capricorn-final-analysis/.env.yaml"

# Create capricorn-feedback/.env.yaml
if [ -z "$SENDGRID_API_KEY" ]; then
    cat > backend/capricorn-feedback/.env.yaml << EOF
# Environment variables for capricorn-feedback Cloud Function
SENDGRID_API_KEY: "YOUR_SENDGRID_API_KEY"  # TODO: Add your SendGrid API key
EOF
    echo "✓ Created backend/capricorn-feedback/.env.yaml (SendGrid API key needs to be added)"
else
    cat > backend/capricorn-feedback/.env.yaml << EOF
# Environment variables for capricorn-feedback Cloud Function
SENDGRID_API_KEY: "$SENDGRID_API_KEY"
EOF
    echo "✓ Created backend/capricorn-feedback/.env.yaml"
fi

# Create pubmed-search-tester-extract-disease/.env.yaml
cat > backend/pubmed-search-tester-extract-disease/.env.yaml << EOF
# Environment variables for pubmed-search-tester-extract-disease Cloud Function
PROJECT_ID: "$PROJECT_ID"
LOCATION: "$REGION"
EOF
echo "✓ Created backend/pubmed-search-tester-extract-disease/.env.yaml"

# Create pubmed-search-tester-extract-events/.env.yaml
cat > backend/pubmed-search-tester-extract-events/.env.yaml << EOF
# Environment variables for pubmed-search-tester-extract-events Cloud Function
PROJECT_ID: "$PROJECT_ID"
LOCATION: "$REGION"
EOF
echo "✓ Created backend/pubmed-search-tester-extract-events/.env.yaml"

chmod 600 backend/*/.env.yaml 2>/dev/null || true

echo ""
echo "=================================================="
echo "Configuration files created successfully!"
echo "=================================================="
echo ""
echo "Summary:"
echo "- Project ID: $PROJECT_ID"
echo "- Database ID: $DATABASE_ID"
echo "- Region: $REGION"
echo "- BigQuery Project ID: $BIGQUERY_PROJECT_ID"

if [ -z "$SENDGRID_API_KEY" ]; then
    echo ""
    echo "⚠️  Note: Remember to add your SendGrid API key to:"
    echo "   backend/capricorn-feedback/.env.yaml"
fi

echo ""
echo "Next steps:"
echo "1. Review the generated .env.yaml files"
echo "2. Deploy the Cloud Functions using:"
echo "   ./deploy-all-functions.sh"
echo ""
