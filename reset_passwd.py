#!/usr/bin/env python3

import requests
import csv
import io
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Google Sheet URL for password reset data
GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1FwKV4qlV00rcwfjYx4Nhw31Pkd5HxnhJglWVr9ieSQc/edit?usp=sharing"

# Thread locks for thread safety
sheet_update_lock = threading.Lock()
data_processing_lock = threading.Lock()

def read_google_sheet():
    """Read data from Google Sheet, apply filters, and return header and filtered rows"""
    try:
        # Convert the sharing URL to CSV export URL
        csv_url = GOOGLE_SHEET_CSV_URL.replace('/edit?usp=sharing', '/export?format=csv&gid=0')
        
        response = requests.get(csv_url)
        response.raise_for_status()
        
        csv_file = io.StringIO(response.text)
        csv_reader = list(csv.reader(csv_file))
        
        if not csv_reader:
            print("No data found in the Google Sheet")
            return None, None
            
        header = csv_reader[0]
        rows = csv_reader[1:]
        
        print(f"Header: {header}")
        print(f"Found {len(rows)} rows of data before filtering")
        
        # Find column indices for filtering
        order_status_index = None
        setup_admin_index = None
        activation_url_index = None
        status_index = None
        password_index = None
        domain_index = 0  # Assuming domain is in the first column
        
        for i, col_name in enumerate(header):
            if 'order_status' in col_name.lower():
                order_status_index = i
            elif 'setup_admin' in col_name.lower():
                setup_admin_index = i
            elif 'activation' in col_name.lower() and 'url' in col_name.lower():
                activation_url_index = i
            elif 'status' in col_name.lower() and 'order' not in col_name.lower():
                status_index = i
            elif 'password' in col_name.lower() and 'admin' in col_name.lower():
                password_index = i
        
        # Validate required columns
        required_columns = [
            (order_status_index, "order_status"),
            (setup_admin_index, "setup_admin"),
            (activation_url_index, "activation_url"),
            (status_index, "status"),
            (password_index, "admin_password")
        ]
        
        for index, col_name in required_columns:
            if index is None:
                print(f"Could not find '{col_name}' column in header: {header}")
                return None, None
        
        # Filter rows based on conditions
        filtered_rows = [
            row for row in rows
            if (len(row) > max(order_status_index, setup_admin_index, activation_url_index, status_index, password_index) and
                row[order_status_index].lower() == 'success' and
                row[setup_admin_index].lower() == 'success' and
                row[activation_url_index].strip() != '' and
                (row[status_index].strip() == '' or row[status_index].lower() == 'empty'))
        ]
        
        print(f"After filtering: {len(filtered_rows)} rows remain")
        
        # Print fetched and filtered data
        print("\n📋 Fetched and Filtered Data:")
        print("=" * 50)
        for i, row in enumerate(filtered_rows, 1):
            domain = row[domain_index] if len(row) > domain_index else "N/A"
            order_status = row[order_status_index] if len(row) > order_status_index else "N/A"
            setup_admin = row[setup_admin_index] if len(row) > setup_admin_index else "N/A"
            activation_url = row[activation_url_index] if len(row) > activation_url_index else "N/A"
            status = row[status_index] if len(row) > status_index else "N/A"
            admin_password = row[password_index] if len(row) > password_index else "N/A"
            
            print(f"Row {i}:")
            print(f"  Domain: {domain}")
            print(f"  Order Status: {order_status}")
            print(f"  Setup Admin: {setup_admin}")
            print(f"  Activation URL: {activation_url}")
            print(f"  Status: {status}")
            print(f"  Admin Password: {admin_password}")
            print("-" * 50)
        
        return header, filtered_rows
        
    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        return None, None

def extract_password_and_url(row, header):
    """Extract Admin Password and activation_url from a row"""
    try:
        # Find column indices
        password_index = None
        url_index = None
        
        for i, col_name in enumerate(header):
            if 'password' in col_name.lower() and 'admin' in col_name.lower():
                password_index = i
            elif 'activation' in col_name.lower() and 'url' in col_name.lower():
                url_index = i
        
        if password_index is None or url_index is None:
            print(f"Could not find required columns in header: {header}")
            return None, None
            
        # Extract values
        admin_password = row[password_index] if len(row) > password_index else ""
        activation_url = row[url_index] if len(row) > url_index else ""
        
        return admin_password, activation_url
        
    except Exception as e:
        print(f"Error extracting data from row: {e}")
        return None, None

def process_password_change(url, admin_password, domain_name, row_index):
    """Complete password change process: click I understand, fill password, submit, and update status"""
    driver = None
    try:
        # Validate input parameters
        if not url or not admin_password or not domain_name:
            print(f"❌ Invalid parameters: URL={url}, Password={'*' * len(admin_password) if admin_password else 'None'}, Domain={domain_name}")
            return False
            
        print(f"\n🤖 Processing {domain_name}")
        print(f"🔑 Admin Password: {admin_password}")
        print(f"🌐 URL: {url}")
        
        # Initialize browser in private/incognito mode
        driver = Driver(uc=True, incognito=True, headless=False)
        driver.maximize_window()
        
        # Navigate to activation URL
        driver.get(url)
        time.sleep(3)
        
        print(f"📄 Page loaded: {driver.title}")
        
        # Step 1: Find and click the "I understand" button
        google_confirm_selectors = [
            "input[type='submit'][name='confirm'][value='I understand']",
            "input[name='confirm'][value='I understand']",
            "input[value='I understand']",
            "input[type='submit'][id='confirm']",
            "input[id='confirm']",
            "input[class*='MK9CEd'][class*='MVpUfe']",
            "input[jsname='M2UYVd']"
        ]
        
        button_clicked = False
        for selector in google_confirm_selectors:
            try:
                confirm_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                confirm_btn.click()
                print(f"✅ 'I understand' button clicked for {domain_name}")
                button_clicked = True
                break
            except TimeoutException:
                continue
        
        if not button_clicked:
            print(f"⚠️  Could not find 'I understand' button for {domain_name}")
            return False
        
        # Wait for password change page to load
        time.sleep(3)
        print(f"📄 After I understand - Page loaded: {driver.title}")
        
        # Step 2: Fill in the password fields
        password_selectors = [
            "input[type='password']",
            "input[name*='password']",
            "input[id*='password']",
            "input[placeholder*='password']"
        ]
        
        password_inputs = []
        for selector in password_selectors:
            try:
                inputs = driver.find_elements(By.CSS_SELECTOR, selector)
                password_inputs.extend(inputs)
            except:
                continue
        
        if len(password_inputs) >= 2:
            # Fill first password field (Create password)
            password_inputs[0].clear()
            password_inputs[0].send_keys(admin_password)
            print(f"✅ First password field filled")
            
            # Fill second password field (Confirm password)
            password_inputs[1].clear()
            password_inputs[1].send_keys(admin_password)
            print(f"✅ Second password field filled")
        else:
            print(f"⚠️  Could not find password input fields")
            return False
        
        # Step 3: Click "Change password" button
        change_password_selectors = [
            "button:contains('Change password')",
            "input[value='Change password']",
            "button[type='submit']",
            ".btn-primary",
            "button:contains('Submit')"
        ]
        
        change_clicked = False
        for selector in change_password_selectors:
            try:
                if ":contains" in selector:
                    # Handle XPath for text content
                    xpath = f"//button[contains(text(), 'Change password')]"
                    change_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, xpath))
                    )
                else:
                    change_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                change_btn.click()
                print(f"✅ 'Change password' button clicked")
                change_clicked = True
                break
            except TimeoutException:
                continue
        
        if not change_clicked:
            print(f"⚠️  Could not find 'Change password' button")
            return False
        
        # Step 4: Update status as success after clicking "Change password"
        print(f"✅ Password change completed for {domain_name}")
        update_status_in_sheet(row_index, "success", domain_name)
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing {domain_name}: {e}")
        # Update status as error
        update_status_in_sheet(row_index, "error", domain_name)
        return False
        
    finally:
        if driver:
            # Close the browser
            try:
                driver.quit()
                print(f"🌐 Browser closed for {domain_name}")
            except Exception as e:
                print(f"⚠️  Error closing browser for {domain_name}: {e}")

def update_status_in_sheet(row_index, status, domain_name=None):
    """Update status in Google Sheet via Apps Script using row index (thread-safe)"""
    with sheet_update_lock:
        try:
            apps_script_url = "https://script.google.com/macros/s/AKfycbx5HN2pQpDTJmdSAxMPJi4NtzkDtH0MPklm1i1xHH7RkXjNV4kp1R85nDs7burLGNqg/exec"
            
            data = {
                "row_index": row_index,  # Use row index instead of domain name
                "status": status
            }
            
            # Include domain name for logging purposes if provided
            if domain_name:
                data["domain"] = domain_name
            
            response = requests.post(apps_script_url, json=data)
            
            if response.status_code == 200:
                log_msg = f"✅ Status updated in sheet: Row {row_index}"
                if domain_name:
                    log_msg += f" ({domain_name})"
                log_msg += f" -> {status}"
                print(log_msg)
            else:
                print(f"⚠️  Failed to update status in sheet: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error updating status in sheet: {e}")

def process_single_domain(domain_data):
    """Process a single domain (for concurrent execution)"""
    # Extract data safely (no need for lock since we're working with immutable copies)
    domain_name, admin_password, activation_url, index, total = domain_data
    
    # Get current thread info for debugging
    thread_id = threading.current_thread().ident
    thread_name = threading.current_thread().name
    
    print(f"\n{'='*20} Thread {thread_name} ({thread_id}) Processing {index}/{total} {'='*20}")
    print(f"🏢 Domain: {domain_name}")
    print(f"🔑 Admin Password: {admin_password}")
    print(f"🌐 Activation URL: {activation_url}")
    
    if not admin_password or not activation_url:
        print(f"⚠️  Skipping {domain_name} - missing password or URL")
        return False
    
    try:
        return process_password_change(activation_url, admin_password, domain_name, index)
    except Exception as e:
        print(f"❌ Error processing {domain_name}: {e}")
        return False

def main():
    """Main function to process activation URLs with concurrent execution"""
    print("🚀 Starting Concurrent Activation Process")
    print("=" * 50)
    
    # Read data from Google Sheet
    header, rows = read_google_sheet()
    
    if not header or not rows:
        print("❌ Failed to read data from Google Sheet")
        return
    
    print(f"\n📊 Processing {len(rows)} entries with 10 concurrent threads...")
    
    # Prepare domain data for concurrent processing (create immutable copies)
    domain_data_list = []
    for i, row in enumerate(rows, 1):
        if not row or len(row) < 2:  # Skip empty rows
            continue
            
        # Extract domain name (first column)
        domain_name = row[0] if row[0] else f"Entry {i}"
        
        # Extract password and URL
        admin_password, activation_url = extract_password_and_url(row, header)
        
        if admin_password and activation_url:
            # Create a tuple with immutable data for each thread
            # Format: (domain_name, admin_password, activation_url, row_index, total_rows)
            domain_data = (
                str(domain_name),      # Ensure string copy
                str(admin_password),   # Ensure string copy  
                str(activation_url),   # Ensure string copy
                int(i),               # Row index for sheet updates (1-based)
                int(len(rows))        # Total rows count
            )
            domain_data_list.append(domain_data)
    
    if not domain_data_list:
        print("❌ No valid domains found to process")
        return
    
    print(f"🎯 Found {len(domain_data_list)} valid domains to process")
    
    # Validate domain data before processing
    print("\n📋 Domain Data Validation:")
    for i, domain_data in enumerate(domain_data_list, 1):
        domain_name, admin_password, activation_url, index, total = domain_data
        print(f"  {i}. {domain_name} -> Password: {admin_password[:8]}... -> URL: {activation_url[:50]}...")
    
    # Process domains concurrently with ThreadPoolExecutor
    max_workers = 10  # Maximum 10 concurrent threads
    successful_count = 0
    failed_count = 0
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_domain = {
                executor.submit(process_single_domain, domain_data): domain_data[0] 
                for domain_data in domain_data_list
            }
            
            # Process completed tasks
            for future in as_completed(future_to_domain):
                domain_name = future_to_domain[future]
                try:
                    result = future.result()
                    if result:
                        successful_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    print(f"❌ Exception for {domain_name}: {e}")
                    failed_count += 1
                
                print(f"📊 Progress: {successful_count + failed_count}/{len(domain_data_list)} completed")
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Process interrupted by user")
    
    print(f"\n✅ Concurrent activation process completed!")
    print(f"📊 Results: {successful_count} successful, {failed_count} failed")
    print("=" * 50)

if __name__ == "__main__":
    main()
