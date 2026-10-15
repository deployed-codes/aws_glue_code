from pathlib import Path

from glue_lib.bootstrap import GlueBootstrap
from .job import Nu0cNissAuto115AtprmFinalLoadJob

CONFIG_PATH = Path(__file__).with_name("config.yaml")

def main() -> None:

    bootstrap = GlueBootstrap()
    context = bootstrap.initialize(CONFIG_PATH)

    try:
        job = Nu0cNissAuto115AtprmFinalLoadJob(context)
        job.execute()
    finally:
        context.runtime.close()

if __name__ == "__main__":
    main()
