import datetime

from pathlib import Path
import pytz

def list_files(directory: str):
    return [p for p in Path(directory).iterdir() if p.is_file()]

# files: List[Path]
def graveyard_files(directory: Path, files, verbose=2):

    # create graveyard directory if it doesn't exist
    Path(directory / 'graveyard').mkdir(parents=False, exist_ok=True)
    graveyard = Path(f'{directory}/graveyard')

    # move files
    for f in files:
        destination = graveyard / f.name
        print(f'Moving file {f.resolve()} to {destination}')
        f.rename(destination)

def dateCETstr_to_tzdt(date: str, format='%Y%m%d'):
    date = datetime.datetime.strptime(date, '%Y%m%d')
    time = pytz.timezone('Europe/Madrid').localize(date)
    time = time.astimezone(datetime.timezone.utc)
    return time
