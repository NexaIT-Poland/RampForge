#!/bin/bash
echo "=== Testing Rate Limiting ==="
echo "Sending 7 login requests (limit is 5/minute)..."
echo ""

for i in {1..7}; do
  echo -n "Request $i: "
  http_code=$(curl -s -w "%{http_code}" -o /dev/null -X POST http://localhost:8000/api/auth/login \
    -H "Content-Type: application/json" \
    -d '{"email":"test@test.com","password":"wrong"}')

  if [ "$http_code" == "429" ]; then
    echo "HTTP $http_code - ✅ Rate Limited (expected)"
  elif [ "$http_code" == "401" ]; then
    echo "HTTP $http_code - ✅ Auth failed (request processed)"
  else
    echo "HTTP $http_code"
  fi

  sleep 0.1
done

echo ""
echo "Expected: First 5 requests = HTTP 401, Requests 6-7 = HTTP 429"
