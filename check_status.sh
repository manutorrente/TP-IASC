#!/bin/bash
# Shell script to check the status of all TP-IASC instances

echo -e "\033[0;32mTP-IASC System Status Check\033[0m"
echo -e "\033[0;32m==================================================\033[0m"

# Function to check if a port is listening
test_port() {
    local host="$1"
    local port="$2"
    local service_name="$3"
    
    if nc -z -w 2 "$host" "$port" 2>/dev/null; then
        echo -e "\033[0;32m✓ $service_name ($host:$port) - RUNNING\033[0m"
        return 0
    else
        echo -e "\033[0;31m✗ $service_name ($host:$port) - NOT RUNNING\033[0m"
        return 1
    fi
}

# Function to make HTTP health check
test_health_endpoint() {
    local url="$1"
    local service_name="$2"
    
    local response=$(curl -s -m 5 "$url/health" 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        local status=$(echo "$response" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
        if [ "$status" = "ok" ]; then
            echo -e "\033[0;32m✓ $service_name - HEALTHY\033[0m"
        else
            echo -e "\033[0;33m? $service_name - UNKNOWN STATUS\033[0m"
        fi
    else
        echo -e "\033[0;31m✗ $service_name - HEALTH CHECK FAILED\033[0m"
    fi
}

# Check if required commands are available
if ! command -v nc &> /dev/null; then
    echo -e "\033[0;33mWarning: 'nc' (netcat) not found. Port checks may not work.\033[0m"
    echo -e "\033[0;33mInstall it with: brew install netcat (macOS) or apt-get install netcat (Linux)\033[0m"
fi

if ! command -v curl &> /dev/null; then
    echo -e "\033[0;33mWarning: 'curl' not found. Health checks may not work.\033[0m"
fi

echo -e "\n\033[0;36mChecking App Instances:\033[0m"
test_port "localhost" 9000 "App Instance 1"
test_port "localhost" 9001 "App Instance 2"
test_port "localhost" 9002 "App Instance 3"

echo -e "\n\033[0;36mChecking Sentinel Instances:\033[0m"
test_port "localhost" 8000 "Sentinel Instance 1"
test_port "localhost" 8001 "Sentinel Instance 2"
test_port "localhost" 8002 "Sentinel Instance 3"

echo -e "\n\033[0;36mHealth Check (if instances are running):\033[0m"
test_health_endpoint "http://localhost:9000" "App 1 Health"
test_health_endpoint "http://localhost:9001" "App 2 Health"
test_health_endpoint "http://localhost:9002" "App 3 Health"
test_health_endpoint "http://localhost:8000" "Sentinel 1 Health"
test_health_endpoint "http://localhost:8001" "Sentinel 2 Health"
test_health_endpoint "http://localhost:8002" "Sentinel 3 Health"

echo -e "\n\033[0;36mCluster Status (from Sentinel 1):\033[0m"
cluster_response=$(curl -s -m 5 "http://localhost:8000/cluster-status" 2>/dev/null)

if [ $? -eq 0 ] && [ -n "$cluster_response" ]; then
    echo -e "\033[0;37mCluster status retrieved successfully:\033[0m"
    echo "$cluster_response" | python3 -m json.tool 2>/dev/null || echo "$cluster_response"
else
    echo -e "\033[0;31m✗ Could not retrieve cluster status from Sentinel 1\033[0m"
fi

echo -e "\n\033[0;33mUseful URLs:\033[0m"
echo -e "\033[0;37mApp instances:\033[0m"
echo "  - http://localhost:9000/health"
echo "  - http://localhost:9001/health"
echo "  - http://localhost:9002/health"
echo -e "\033[0;37mSentinel monitoring:\033[0m"
echo "  - http://localhost:8000/cluster-status"
echo "  - http://localhost:8001/cluster-status"
echo "  - http://localhost:8002/cluster-status"
