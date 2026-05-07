#!/bin/bash
curl -X POST \
  "https://avpizwlpceuuawxaiwbv.supabase.co/functions/v1/whatsapp-alerts" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cGl6d2xwY2V1dWF3eGFpd2J2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Nzk3MzU3MCwiZXhwIjoyMDkzNTQ5NTcwfQ.3eBjeMu8uNZQOY1dJNr7BMQ-WIaR0wYMF65OLQQ6DqM" \
  -H "Content-Type: application/json" \
  -d '{}'
