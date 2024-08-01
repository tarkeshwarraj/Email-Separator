import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_line(line, email_file, seen_emails, progress_bar):
    line = line.strip()
    if ':' in line:
        email, _ = line.split(':', 1)
    elif '|' in line:
        email, _ = line.split('|', 1)
    else:
        return  # Skip lines that don't contain a separator
    
    # Check for duplicates
    if email not in seen_emails:
        seen_emails.add(email)
        with open(email_file, 'a') as efile:
            efile.write(email + '\n')
        # Print each email as it's processed
        progress_bar[1] = email  # Update the most recent email

    # Update progress
    progress_bar[0] += 1
    print(f"Processed {progress_bar[0]} lines - Last email: {progress_bar[1]}", end='\r')

def separate_logs(input_file, email_file, num_threads):
    seen_emails = set()
    
    with open(input_file, 'r') as infile:
        lines = infile.readlines()

    total_lines = len(lines)
    progress_bar = [0, ""]  # Use a list to pass by reference: [line_count, last_email]

    print(f"Processing file: {input_file}")
    
    # Use ThreadPoolExecutor with the specified number of threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_line, line, email_file, seen_emails, progress_bar) for line in lines]
        # Wait for all threads to complete
        for future in as_completed(futures):
            future.result()  # This will re-raise any exceptions that occurred during processing

    print(f"\nProcessing complete. {total_lines} lines processed.")

# Define directories
script_dir = os.path.dirname(__file__)
task_folder = os.path.join(script_dir, 'task')
result_folder = os.path.join(script_dir, 'result')

# Ensure the result folder exists
os.makedirs(result_folder, exist_ok=True)

# Get all files from the task folder
task_files = [f for f in os.listdir(task_folder) if os.path.isfile(os.path.join(task_folder, f))]
print(f"Files found: {task_files}")
if not task_files:
    raise FileNotFoundError("No files found in the task folder.")

# Ask the user for the number of threads
num_threads = int(input("Please enter the number of threads to use: "))

# Process each file in the task folder
for task_file in task_files:
    input_log_file = os.path.join(task_folder, task_file)

    # Generate a timestamped result filename based on the task file name
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_name, ext = os.path.splitext(task_file)
    email_output_file = os.path.join(result_folder, f'{base_name}_{timestamp}{ext}')

    # Process the file
    separate_logs(input_log_file, email_output_file, num_threads)
