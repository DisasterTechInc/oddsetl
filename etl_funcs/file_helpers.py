import shutil

def cleanup_the_house(dir_lst):
    """Flush dir and subdirs."""

    for dir_to_remove in dirs_lst:
        shutil.rmtree(dir_to_remove)

    return




