import re

LOG_FILE = "application.log"

def extract_errors(log_file):
    errors = []
    current_error = []

    with open(log_file, "r", encoding="utf-8", errors="ignore") as f:

        for line in f:

            # Detect start of exception
            if ("Exception" in line or "ERROR" in line or "Caused by" in line):

                if current_error:
                    errors.append(current_error)

                current_error = [line]

            elif current_error:
                current_error.append(line)

                # Stop when empty line occurs
                if line.strip() == "":
                    errors.append(current_error)
                    current_error = []

    return errors


def find_root_cause(errors):

    for idx, error in enumerate(errors):

        print(f"\n{'='*80}")
        print(f"ERROR BLOCK #{idx+1}")
        print(f"{'='*80}")

        full_error = "".join(error)

        # Extract caused by
        caused_by = re.findall(r"Caused by:\s+(.*)", full_error)

        if caused_by:
            print("\nROOT CAUSE:")
            print(caused_by[-1])

        else:
            print("\nNO ROOT CAUSE FOUND")

        print("\nSTACK TRACE:")
        print(full_error[:5000])  # limit output


if __name__ == "__main__":

    errors = extract_errors(LOG_FILE)

    print(f"Total Error Blocks Found: {len(errors)}")

    find_root_cause(errors)
