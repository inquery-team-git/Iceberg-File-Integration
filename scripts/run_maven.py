from pathlib import Path
import subprocess

# Determine the path to the Maven project by getting the parent directory of the current script's directory
current_script_path = Path(__file__).resolve()
maven_project_path = current_script_path.parent.parent
def run_maven_command(main_class, args_list):
    """
    Runs a Maven command with the specified main class and arguments.
    
    Parameters:
        main_class (str): The fully qualified name of the main class to run.
        args (str): The arguments as a single string to pass to the Java application.
    """
    import subprocess
    
    # Construct the Maven command
    mvn_command = [
        "mvn", 
        "exec:java",
        "-f", str(maven_project_path / "pom.xml"),  # Use the Path object to specify the pom.xml path
        f"-Dexec.mainClass={main_class}",
    ]
    mvn_command.append("-Dexec.args=" + ' '.join(f'"{arg}"' for arg in args_list))

    print("Executing command:", ' '.join(mvn_command))
    
    try:
        # Run the Maven command
        result = subprocess.run(mvn_command, capture_output=True, text=True)
        print("Output:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running Maven command:", e, e.output)

def run_directory_schema_reader(directory_path, database_name, table_name):
    args_list = [directory_path, database_name, table_name]
    run_maven_command("DirectorySchemaReader", args_list)

def run_new_file_appender(file_path, database_name, table_name):
    args_list = [file_path, database_name, table_name]
    run_maven_command("newFileAppender", args_list)

def run_create_table(file_path, database_name, table_name):
    args_list = [file_path, database_name, table_name]

    run_maven_command("CreateTable", args_list)

# Path to a file that you want to upload. 
# This file will be used to initialize the table schema on creation.
file_path = "s3a://**/my_directory/my_file.parquet"

# Path to the directory containing the rest of the files you want to upload.
# These files' schemas will not be validated on upload.
directory_path = "s3a://**/my_directory/" 

# Name of the database where you want your table to live.
database_name = "db_name"

# Name of the table you want to use or create.
table_name = "table_name"

run_create_table(file_path, database_name, table_name)
run_new_file_appender(file_path, database_name, table_name)
run_directory_schema_reader(directory_path, database_name, table_name)

