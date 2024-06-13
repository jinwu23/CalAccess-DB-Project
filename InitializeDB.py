import subprocess

# List of Python files to run in order
scripts = ['Filer.py', 
           'Persons.py', 
           'Lobbyists.py', 
           'Bills.py', 
           'Organizations.py',
           'LobbyingFirms.py',
           'HireFirm.py'
           ]

# Iterate over the scripts and run each one
for script in scripts:
    print(f"Running {script}...")
    result = subprocess.run(['python', script], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {script}: {result.stderr}")
    else:
        print(f"Finished running {script}")

print("All scripts executed.")
