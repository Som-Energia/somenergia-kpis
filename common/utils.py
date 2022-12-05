import datetime
import pendulum

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
        if verbose > 1:
            print(f'Moving file {f.resolve()} to {destination}')
        f.rename(destination)

def restore_files(directory: Path, verbose=2):
    graveyard = Path(f'{directory}/graveyard')

    for f in graveyard.iterdir():
        if f.is_file:
            destination = graveyard / '..' / f.name
            destination = destination.resolve()
            if verbose > 1:
                print(f'Restoring file {f.resolve()} to {destination}')
            f.rename(destination)


def dateCETstr_to_tzdt(date: str, format='%Y%m%d'):
    date = datetime.datetime.strptime(date, format)
    time = pytz.timezone('Europe/Madrid').localize(date)
    time = time.astimezone(datetime.timezone.utc)
    return time

def dateCETstr_to_CETtzdt(date: str, format='%Y%m%d'):
    date = datetime.datetime.strptime(date, format)
    time = pytz.timezone('Europe/Madrid').localize(date)
    return time

def to_iso(date):
    return pendulum.parse(date).in_tz('UTC').to_iso8601_string()