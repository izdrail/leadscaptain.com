import json
import sqlite3

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('people_data.db')
cursor = conn.cursor()

# Create tables with structure based on the JSON fields
cursor.execute('''
    CREATE TABLE IF NOT EXISTS people (
        id TEXT PRIMARY KEY,
        full_name TEXT,
        first_name TEXT,
        middle_initial TEXT,
        middle_name TEXT,
        last_name TEXT,
        gender TEXT,
        birth_year INTEGER,
        birth_date TEXT,
        linkedin_url TEXT,
        linkedin_username TEXT,
        linkedin_id TEXT,
        facebook_url TEXT,
        facebook_username TEXT,
        facebook_id TEXT,
        twitter_url TEXT,
        twitter_username TEXT,
        github_url TEXT,
        github_username TEXT,
        work_email TEXT,
        mobile_phone TEXT,
        industry TEXT,
        job_title TEXT,
        job_title_role TEXT,
        job_title_sub_role TEXT,
        job_company_name TEXT,
        job_last_updated TEXT,
        job_start_date TEXT,
        location_name TEXT,
        location_country TEXT,
        location_continent TEXT,
        linkedin_connections INTEGER,
        inferred_salary TEXT,
        inferred_years_experience INTEGER,
        summary TEXT
    )
''')

# Create the jobs table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id TEXT,
        job_title TEXT,
        job_title_role TEXT,
        job_title_sub_role TEXT,
        job_company_name TEXT,
        job_start_date TEXT,
        job_end_date TEXT,
        FOREIGN KEY (person_id) REFERENCES people (id)
    )
''')

# Create the emails table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS emails (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id TEXT,
        email_address TEXT,
        email_type TEXT,
        FOREIGN KEY (person_id) REFERENCES people (id)
    )
''')

# Create the education table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id TEXT,
        school_name TEXT,
        start_date TEXT,
        end_date TEXT,
        degrees TEXT,
        majors TEXT,
        FOREIGN KEY (person_id) REFERENCES people (id)
    )
''')

print("Database part-00001 connection established and tables created (if not exist).")

file = input("please provide the file name")
# Open the file and parse the JSON
with open(file, 'r') as file:
    for line_number, line in enumerate(file, start=1):
        print(f"\nProcessing line {line_number}...")
        try:
            data = json.loads(line)

            # Insert person data into the people table
            cursor.execute('''
                INSERT OR REPLACE INTO people (id, full_name, first_name, middle_initial, middle_name, last_name, gender, birth_year, birth_date, linkedin_url, linkedin_username, linkedin_id, facebook_url, facebook_username, facebook_id, twitter_url, twitter_username, github_url, github_username, work_email, mobile_phone, industry, job_title, job_title_role, job_title_sub_role, job_company_name, job_last_updated, job_start_date, location_name, location_country, location_continent, linkedin_connections, inferred_salary, inferred_years_experience, summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('id'), data.get('full_name'), data.get('first_name'), data.get('middle_initial'),
                data.get('middle_name'), data.get('last_name'), data.get('gender'), data.get('birth_year'),
                data.get('birth_date'), data.get('linkedin_url'), data.get('linkedin_username'), data.get('linkedin_id'),
                data.get('facebook_url'), data.get('facebook_username'), data.get('facebook_id'), data.get('twitter_url'),
                data.get('twitter_username'), data.get('github_url'), data.get('github_username'), data.get('work_email'),
                data.get('mobile_phone'), data.get('industry'), data.get('job_title'), data.get('job_title_role'),
                data.get('job_title_sub_role'), data.get('job_company_name'), data.get('job_last_updated'),
                data.get('job_start_date'), data.get('location_name'), data.get('location_country'), data.get('location_continent'),
                data.get('linkedin_connections'), data.get('inferred_salary'), data.get('inferred_years_experience'), data.get('summary')
            ))

            print(f"Line {line_number}: Person data inserted successfully.")

            # Insert emails
            for email in data.get('emails', []):
                cursor.execute('''
                    INSERT INTO emails (person_id, email_address, email_type)
                    VALUES (?, ?, ?)
                ''', (data.get('id'), email.get('address'), email.get('type')))

            print(f"Line {line_number}: Emails data inserted successfully.")

            # Insert job experience
            for job in data.get('experience', []):
                cursor.execute('''
                    INSERT INTO jobs (person_id, job_title, job_title_role, job_title_sub_role, job_company_name, job_start_date, job_end_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data.get('id'), job['title']['name'], job['title'].get('role'), job['title'].get('sub_role'),
                    job['company']['name'], job.get('start_date'), job.get('end_date')
                ))

            print(f"Line {line_number}: Job data inserted successfully.")

            # Insert education details
            for edu in data.get('education', []):
                school_name = edu['school']['name'] if edu.get('school') else None
                degrees = ','.join(edu.get('degrees', []))
                majors = ','.join(edu.get('majors', []))

                cursor.execute('''
                    INSERT INTO education (person_id, school_name, start_date, end_date, degrees, majors)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    data.get('id'), school_name, edu.get('start_date'), edu.get('end_date'), degrees, majors
                ))

            print(f"Line {line_number}: Education data inserted successfully.")

            # Commit each line
            conn.commit()

        except json.JSONDecodeError:
            print(f"Line {line_number}: Failed to parse JSON.")
        except Exception as e:
            print(f"Line {line_number}: Error occurred - {e}")

print("Data processing completed.")
conn.close()

