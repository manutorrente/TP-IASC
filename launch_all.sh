#!/bin/bash
# Shell script to launch multiple sentinel and app instances
# Each instance runs in its own terminal window for easy monitoring

echo -e "\033[0;32mStarting TP-IASC distributed system...\033[0m"
echo -e "\033[0;33mThis will launch 3 Sentinel instances and 3 App instances in separate terminal windows\033[0m"

# Get the current directory (project root)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to start a new terminal window with a specific command
start_new_terminal() {
    local title="$1"
    local command="$2"
    
    # Detect OS and use appropriate terminal command
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        osascript <<EOF
tell application "Terminal"
    do script "cd '$PROJECT_ROOT' && $command"
    set custom title of front window to "$title"
end tell
EOF
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux - try common terminal emulators
        if command -v gnome-terminal &> /dev/null; then
            gnome-terminal --title="$title" -- bash -c "cd '$PROJECT_ROOT' && $command; exec bash"
        elif command -v xterm &> /dev/null; then
            xterm -T "$title" -e "cd '$PROJECT_ROOT' && $command; bash" &
        elif command -v konsole &> /dev/null; then
            konsole --title "$title" -e bash -c "cd '$PROJECT_ROOT' && $command; exec bash" &
        else
            echo "No supported terminal emulator found. Running in background..."
            cd "$PROJECT_ROOT" && $command &
        fi
    else
        echo "Unsupported OS. Running in background..."
        cd "$PROJECT_ROOT" && $command &
    fi
    
    echo -e "\033[0;36mStarted: $title\033[0m"
}

# Launch App instances (ports 9000, 9001, 9002)
echo -e "\n\033[0;33mLaunching App instances...\033[0m"

start_new_terminal "App Instance - Port 9000" "uv run app/main.py 9000"
sleep 1

start_new_terminal "App Instance - Port 9001" "uv run app/main.py 9001"
sleep 1

start_new_terminal "App Instance - Port 9002" "uv run app/main.py 9002"
sleep 2

# Launch Sentinel instances (using config0.yaml, config1.yaml, config2.yaml)
echo -e "\n\033[0;33mLaunching Sentinel instances...\033[0m"

start_new_terminal "Sentinel Instance - Config 0 (Port 8000)" "uv run -m sentinel.main sentinel/configs/config0.yaml"
sleep 1

start_new_terminal "Sentinel Instance - Config 1 (Port 8001)" "uv run -m sentinel.main sentinel/configs/config1.yaml"
sleep 1

start_new_terminal "Sentinel Instance - Config 2 (Port 8002)" "uv run -m sentinel.main sentinel/configs/config2.yaml"

echo -e "\n\033[0;32mAll instances launched!\033[0m"
echo -e "\n\033[0;37mApp instances running on:\033[0m"
echo -e "\033[0;36m  - http://localhost:9000\033[0m"
echo -e "\033[0;36m  - http://localhost:9001\033[0m"
echo -e "\033[0;36m  - http://localhost:9002\033[0m"

echo -e "\n\033[0;37mSentinel instances running on:\033[0m"
echo -e "\033[0;36m  - http://localhost:8000 (monitoring app on 9001)\033[0m"
echo -e "\033[0;36m  - http://localhost:8001 (monitoring app on 9000)\033[0m"
echo -e "\033[0;36m  - http://localhost:8002 (monitoring app on 9002)\033[0m"

echo -e "\n\033[0;33mTo stop all instances, close the individual terminal windows or press Ctrl+C in each terminal.\033[0m"
echo -e "\033[0;32mYou can also check the cluster status at: http://localhost:8000/cluster-status\033[0m"
