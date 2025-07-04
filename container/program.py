"""
container -> program.py (headmaster) -> delegates task to different execution classes.
each execution class is within the directory
the execution directory and class will be calling the steps defined in steps directory.
"""

from container.execution.aus_comp_pipeline.full_pipeline import execute as aus_comp_ingestion

# for testing purpose args are defined below:
args = {
    "trigger": "aus_comp"
}


def main(args: {}):
    """
        Delegates the control of the program to respected pipeline.
    """
    if args.get("trigger", None) == "aus_comp":
        aus_comp_ingestion()


if __name__ == "__main__":
    "Initiates the program"
    print("Ingestion Process started for aus_comp")
    main(args)
