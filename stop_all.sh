#!/bin/bash
# Shell script to stop all TP-IASC instances

echo -e "\033[0;31mStopping TP-IASC distributed system...\033[0m"

# Function to kill processes by port
stop_process_by_port() {
    local port=$1
    
    # Try to find process using the port
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        local pid=$(lsof -ti:$port 2>/dev/null)
    else
        # Linux
        local pid=$(lsof -ti:$port 2>/dev/null || fuser $port/tcp 2>/dev/null | awk '{print $1}')
    fi
    
    if [ -n "$pid" ]; then
        echo -e "\033[0;33mStopping process on port $port (PID: $pid)\033[0m"
        kill -9 $pid 2>/dev/null
        if [ $? -eq 0 ]; then
            echo -e "\033[0;32m✓ Process on port $port stopped\033[0m"
        else
            echo -e "\033[0;31m✗ Failed to stop process on port $port\033[0m"
        fi
    else
        echo -e "\033[0;90mNo process found on port $port\033[0m"
    fi
}

# Stop all app instances
echo -e "\n\033[0;33mStopping App instances...\033[0m"
stop_process_by_port 9000
stop_process_by_port 9001
stop_process_by_port 9002

# Stop all sentinel instances
echo -e "\n\033[0;33mStopping Sentinel instances...\033[0m"
stop_process_by_port 8000
stop_process_by_port 8001
stop_process_by_port 8002

# Alternative method: kill by process name
echo -e "\n\033[0;33mCleaning up any remaining UV/Python processes...\033[0m"

# Find and kill UV processes related to sentinel or app
pkill -f "uv run.*sentinel" 2>/dev/null && echo -e "\033[0;32m✓ Stopped sentinel UV processes\033[0m"
pkill -f "uv run.*app/main.py" 2>/dev/null && echo -e "\033[0;32m✓ Stopped app UV processes\033[0m"

# Find and kill Python processes related to sentinel or app
pgrep -f "python.*sentinel" | while read pid; do
    echo -e "\033[0;33mStopping Python sentinel process: $pid\033[0m"
    kill -9 $pid 2>/dev/null
done

pgrep -f "python.*app/main.py" | while read pid; do
    echo -e "\033[0;33mStopping Python app process: $pid\033[0m"
    kill -9 $pid 2>/dev/null
done

echo -e "\n\033[0;32mAll TP-IASC instances have been stopped!\033[0m"
