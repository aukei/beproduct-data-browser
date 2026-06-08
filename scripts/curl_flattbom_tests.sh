#!/bin/bash
# curl test scripts for FlatBom endpoint
# Usage: bash scripts/curl_flattbom_tests.sh

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
DOMAIN="lifung"
COMPANY="lifung"
BASE_URL="https://developers.beproduct.com/api/$DOMAIN"

# Get access token from environment or request it
if [ -z "$ACCESS_TOKEN" ]; then
    echo -e "${YELLOW}Note: Set ACCESS_TOKEN environment variable for authentication${NC}"
    echo "Usage: export ACCESS_TOKEN=<your_token> && bash $0"
    echo ""
    echo "To get token:"
    echo "  python3 scripts/get_refresh_token.py"
    exit 1
fi

echo -e "${GREEN}BeProduct FlatBom Endpoint Tests${NC}"
echo "=================================="
echo "Base URL: $BASE_URL"
echo "Company: $COMPANY"
echo ""

# Test style ID (from earlier tests)
STYLE_ID="4a4c5377-b852-434e-92ba-0e9200fa88dd"

echo -e "${YELLOW}Test 1: styleId-Based Request (Report/FlatBom)${NC}"
echo "-------------------------------------------"
echo "Method: POST"
echo "Endpoint: Report/FlatBom"
echo "Content-Type: application/json"
echo "Body: {\"styleId\": \"$STYLE_ID\"}"
echo ""

curl -X 'POST' \
  "$BASE_URL/Report/FlatBom?pageSize=1&pageNumber=1" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
  \"styleId\": \"$STYLE_ID\"
}" -v

echo -e "\n\n"

echo -e "${YELLOW}Test 2: Filters-Based Request (Report/FlatBom) - Expect 500 Error${NC}"
echo "-------------------------------------------------------------------"
echo "Method: POST"
echo "Endpoint: Report/FlatBom"
echo "Content-Type: application/json-patch+json"
echo "Body: filters array with styleID"
echo ""

curl -X 'POST' \
  "$BASE_URL/Report/FlatBom?pageSize=1&pageNumber=1" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json-patch+json' \
  -d "{
  \"filters\": [
    {
      \"field\": \"styleID\",
      \"operator\": \"=\",
      \"value\": \"$STYLE_ID\",
      \"type\": \"String\"
    }
  ]
}" -v

echo -e "\n\n"

echo -e "${YELLOW}Test 3: styleId-Based Request (Style/FlatBom)${NC}"
echo "-------------------------------------------"
echo "Method: POST"
echo "Endpoint: Style/FlatBom"
echo "Content-Type: application/json"
echo ""

curl -X 'POST' \
  "$BASE_URL/Style/FlatBom?pageSize=1&pageNumber=1" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
  \"styleId\": \"$STYLE_ID\"
}" -v

echo -e "\n\n"

echo -e "${YELLOW}Test 4: Filters-Based Request (Style/FlatBom) - Expect 500 Error${NC}"
echo "-------------------------------------------------------------------"
echo "Method: POST"
echo "Endpoint: Style/FlatBom"
echo "Content-Type: application/json-patch+json"
echo ""

curl -X 'POST' \
  "$BASE_URL/Style/FlatBom?pageSize=1&pageNumber=1" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json-patch+json' \
  -d "{
  \"filters\": [
    {
      \"field\": \"styleID\",
      \"operator\": \"Equal\",
      \"value\": \"$STYLE_ID\",
      \"type\": \"String\"
    }
  ]
}" -v

echo -e "\n\n${GREEN}All tests completed${NC}"
