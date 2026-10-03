from airflow.sdk import DAG, task
from datetime import datetime
import csv

INPUT_FILE = "/opt/airflow/data/students.csv"
OUTPUT_FILE = "/opt/airflow/data/student_results.csv"

with DAG(
    dag_id="university_student_data_pipeline",
    description="University experiment for designing, scheduling and monitoring workflows",
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["university", "data-pipeline", "experiment"],
) as dag:

    @task
    def start_pipeline():
        print("Starting Airflow Student Data Pipeline")
        return "Pipeline started"

    @task
    def extract_data():
        students = []
        with open(INPUT_FILE, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                students.append(
                    {
                        "student_id": row["student_id"],
                        "name": row["name"],
                        "subject": row["subject"],
                        "marks": int(row["marks"]),
                    }
                )
        print(f"Extracted {len(students)} student records")
        return students

    @task
    def transform_data(students):
        transformed_students = []
        for student in students:
            marks = student["marks"]
            if marks >= 75:
                grade = "Distinction"
            elif marks >= 60:
                grade = "First Class"
            elif marks >= 40:
                grade = "Pass"
            else:
                grade = "Fail"
            student["grade"] = grade
            transformed_students.append(student)
        print("Student marks transformed successfully")
        return transformed_students

    @task
    def load_data(students):
        fieldnames = [
            "student_id",
            "name",
            "subject",
            "marks",
            "grade",
        ]
        with open(OUTPUT_FILE, "w", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
            )
            writer.writeheader()
            writer.writerows(students)
        print(f"Processed results saved to {OUTPUT_FILE}")
        return OUTPUT_FILE

    @task
    def pipeline_summary(students):
        total = len(students)
        passed = len(
            [
                student
                for student in students
                if student["marks"] >= 40
            ]
        )
        failed = total - passed
        average = sum(
            student["marks"] for student in students
        ) / total
        print("---------- PIPELINE SUMMARY ----------")
        print(f"Total Students : {total}")
        print(f"Passed : {passed}")
        print(f"Failed : {failed}")
        print(f"Average Marks : {average:.2f}")
        print("---------------------------------------")

    start = start_pipeline()
    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)
    summary = pipeline_summary(transformed)

    start >> extracted
    loaded >> summary
