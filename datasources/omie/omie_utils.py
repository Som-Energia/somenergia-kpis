import pandas as pd

def get_file_list(filetype, verbose=2):

    file_listing_base_url = 'https://www.omie.es/es/file-access-list'
    hist_files_url = '{}?realdir={}'.format(file_listing_base_url, filetype)

    try:
        hist_files = pd.read_html(hist_files_url)[0]
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(hist_files['Nombre'][0])

    return hist_files