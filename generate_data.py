import requests
import json
from datetime import datetime, timedelta
import random
import time

# Configuration
BASE_URL = "http://localhost:7000"  # Adjust to your API URL
API_PREFIX = ""  # Adjust if needed

def log(message):
    """Simple logging function"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def create_users():
    """Create mock users"""
    log("Creating users...")
    users = [
        {
            "user_id": "user1",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "organization": "SpaceTech Research"
        },
        {
            "user_id": "user2",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "organization": "Orbital Systems Inc"
        },
        {
            "user_id": "user3",
            "name": "Carol Martinez",
            "email": "carol@example.com",
            "organization": "Satellite Innovations"
        }
    ]
    
    created_users = []
    for user_data in users:
        try:
            response = requests.post(
                f"{BASE_URL}{API_PREFIX}/users",
                json=user_data
            )
            if response.status_code == 200:
                user = response.json()
                created_users.append(user)
                log(f"✓ Created user: {user['name']} ({user['id']})")
            else:
                log(f"✗ Failed to create user {user_data['name']}: {response.status_code}")
        except Exception as e:
            log(f"✗ Error creating user {user_data['name']}: {e}")
    
    return created_users

def create_operators():
    """Create mock operators"""
    log("\nCreating operators...")
    operators = [
        {
            "operator_id": "op1",
            "name": "SkyView Satellites",
            "email": "contact@skyview.com"
        },
        {
            "operator_id": "op2",
            "name": "OrbitWatch Systems",
            "email": "info@orbitwatch.com"
        },
        {
            "operator_id": "op3",
            "name": "StarLink Observatory",
            "email": "operations@starlink-obs.com"
        }
    ]
    
    created_operators = []
    for op_data in operators:
        try:
            response = requests.post(
                f"{BASE_URL}{API_PREFIX}/operators",
                params={
                    "operator_id": op_data["operator_id"],
                    "name": op_data["name"],
                    "email": op_data["email"]
                }
            )
            if response.status_code == 200:
                operator = response.json()
                created_operators.append(operator)
                log(f"✓ Created operator: {operator['name']} ({operator['id']})")
            else:
                log(f"✗ Failed to create operator {op_data['name']}: {response.status_code}")
        except Exception as e:
            log(f"✗ Error creating operator {op_data['name']}: {e}")
    
    return created_operators

def create_windows(operators):
    """Create mock windows for operators"""
    log("\nCreating windows...")
    
    satellite_configs = [
        {
            "satellite_name": "SkyEye-1",
            "satellite_type": "optical_observation",
            "resources": ["optical_channel", "data_transmission"],
            "location": "North America"
        },
        {
            "satellite_name": "RadarSat-5",
            "satellite_type": "radar",
            "resources": ["radar_channel", "data_transmission"],
            "location": "Europe"
        },
        {
            "satellite_name": "CommLink-3",
            "satellite_type": "communications",
            "resources": ["data_transmission"],
            "location": "Asia-Pacific"
        },
        {
            "satellite_name": "TestSat-X",
            "satellite_type": "experimental",
            "resources": ["sensor_a", "sensor_b", "data_transmission"],
            "location": "South America"
        }
    ]
    
    created_windows = []
    for i, operator in enumerate(operators):
        # Create 2-3 windows per operator
        num_windows = random.randint(2, 3)
        for j in range(num_windows):
            config = satellite_configs[random.randint(0, len(satellite_configs) - 1)]
            window_datetime = datetime.utcnow() + timedelta(days=random.randint(1, 30))
            
            window_data = {
                "window_id": f"win_{i}_{j}",
                "satellite_name": config["satellite_name"],
                "satellite_type": config["satellite_type"],
                "resources": config["resources"],
                "window_datetime": window_datetime.isoformat(),
                "offer_duration_minutes": random.choice([30, 60, 120, 180]),
                "location": config["location"]
            }
            
            try:
                response = requests.post(
                    f"{BASE_URL}{API_PREFIX}/operators/{operator['id']}/windows",
                    json=window_data
                )
                if response.status_code == 200:
                    window = response.json()
                    created_windows.append(window)
                    log(f"✓ Created window: {window['satellite_name']} for {operator['name']}")
                else:
                    log(f"✗ Failed to create window: {response.status_code}")
            except Exception as e:
                log(f"✗ Error creating window: {e}")
    
    return created_windows

def create_alerts(users):
    """Create mock alerts for users"""
    log("\nCreating alerts...")
    
    alert_configs = [
        {
            "satellite_types": ["optical_observation"],
            "location": "North America"
        },
        {
            "satellite_types": ["radar", "communications"],
            "start_date": (datetime.utcnow() + timedelta(days=5)).isoformat(),
            "end_date": (datetime.utcnow() + timedelta(days=20)).isoformat()
        },
        {
            "satellite_types": ["experimental"],
            "location": "Europe"
        }
    ]
    
    created_alerts = []
    for i, user in enumerate(users):
        # Create 1-2 alerts per user
        num_alerts = random.randint(1, 2)
        for j in range(num_alerts):
            config = alert_configs[random.randint(0, len(alert_configs) - 1)]
            
            alert_data = {
                "alert_id": f"alert_{i}_{j}",
                "criteria": config
            }
            
            try:
                response = requests.post(
                    f"{BASE_URL}{API_PREFIX}/users/{user['id']}/alerts",
                    json=alert_data
                )
                if response.status_code == 200:
                    alert = response.json()
                    created_alerts.append(alert)
                    log(f"✓ Created alert for {user['name']}")
                else:
                    log(f"✗ Failed to create alert: {response.status_code}")
            except Exception as e:
                log(f"✗ Error creating alert: {e}")
    
    return created_alerts

def create_reservations(users, windows):
    """Create mock reservations"""
    log("\nCreating reservations...")
    
    created_reservations = []
    # Create reservations for some windows
    num_reservations = min(len(windows), len(users) * 2)
    
    for i in range(num_reservations):
        user = users[i % len(users)]
        window = windows[i]
        
        reservation_data = {
            "reservation_id": f"res_{i}",
            "window_id": window["id"]
        }
        
        try:
            response = requests.post(
                f"{BASE_URL}{API_PREFIX}/reservations",
                params={"user_id": user["id"]},
                json=reservation_data
            )
            if response.status_code == 200:
                reservation = response.json()
                created_reservations.append(reservation)
                log(f"✓ Created reservation for {user['name']} on window {window['satellite_name']}")
            else:
                log(f"✗ Failed to create reservation: {response.status_code} - {response.text}")
        except Exception as e:
            log(f"✗ Error creating reservation: {e}")
        
        time.sleep(0.1)  # Small delay to avoid overwhelming the API
    
    return created_reservations

def select_resources(reservations, windows):
    """Select resources for some reservations"""
    log("\nSelecting resources for reservations...")
    
    # Select resources for half of the reservations
    for i, reservation in enumerate(reservations[:len(reservations)//2]):
        # Find the corresponding window
        window = next((w for w in windows if w["id"] == reservation["window_id"]), None)
        if not window or not window.get("resources"):
            continue
        
        # Pick a random available resource
        available_resources = [r for r in window["resources"] if r.get("available", True)]
        if not available_resources:
            continue
        
        resource = random.choice(available_resources)
        
        select_data = {
            "resource_type": resource["type"]
        }
        
        try:
            response = requests.post(
                f"{BASE_URL}{API_PREFIX}/reservations/{reservation['id']}/select-resource",
                json=select_data
            )
            if response.status_code == 200:
                log(f"✓ Selected resource {resource['type']} for reservation {reservation['id']}")
            else:
                log(f"✗ Failed to select resource: {response.status_code}")
        except Exception as e:
            log(f"✗ Error selecting resource: {e}")
        
        time.sleep(0.1)

def cancel_some_reservations(reservations):
    """Cancel some reservations"""
    log("\nCancelling some reservations...")
    
    # Cancel 20% of reservations
    num_to_cancel = max(1, len(reservations) // 5)
    
    for reservation in reservations[:num_to_cancel]:
        try:
            response = requests.post(
                f"{BASE_URL}{API_PREFIX}/reservations/{reservation['id']}/cancel"
            )
            if response.status_code == 200:
                log(f"✓ Cancelled reservation {reservation['id']}")
            else:
                log(f"✗ Failed to cancel reservation: {response.status_code}")
        except Exception as e:
            log(f"✗ Error cancelling reservation: {e}")
        
        time.sleep(0.1)

def main():
    """Main function to populate the API"""
    log("=" * 60)
    log("Starting API Mock Data Population")
    log("=" * 60)
    
    # Create entities in order
    users = create_users()
    operators = create_operators()
    windows = create_windows(operators)
    alerts = create_alerts(users)
    reservations = create_reservations(users, windows)
    
    # Perform actions on reservations
    select_resources(reservations, windows)
    cancel_some_reservations(reservations)
    
    log("\n" + "=" * 60)
    log("Mock Data Population Complete!")
    log("=" * 60)
    log(f"Created: {len(users)} users, {len(operators)} operators, "
        f"{len(windows)} windows, {len(alerts)} alerts, {len(reservations)} reservations")

if __name__ == "__main__":
    main()