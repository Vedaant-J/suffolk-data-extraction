"""
Run all extraction scripts in sequence.
Usage: python run_all.py
"""
import os
import subprocess
import sys
import time

SCRIPTS = [
    ("01_basic_stats.py", "Row counts, schemas, date ranges"),
    ("02_categorical_distributions.py", "Categorical value distributions"),
    ("03_numerical_distributions.py", "Numerical column stats"),
    ("04_incident_correlations.py", "Incident correlation analysis"),
    ("05_sample_rows.py", "Sample rows from all tables"),
]


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    os.chdir(script_dir)

    failed = []
    for script, desc in SCRIPTS:
        print(f"\n{'='*60}")
        print(f"Running: {script} — {desc}")
        print(f"{'='*60}\n")

        t0 = time.time()
        result = subprocess.run(
            [sys.executable, script],
            cwd=script_dir,
        )
        elapsed = time.time() - t0

        if result.returncode != 0:
            print(f"\nFAILED: {script} (exit code {result.returncode})")
            failed.append(script)
        else:
            print(f"\nDone: {script} ({elapsed:.1f}s)")

    print(f"\n{'='*60}")
    if failed:
        print(f"FAILED scripts: {failed}")
    else:
        print("All scripts completed successfully.")
    print(f"Output files in: {output_dir}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
