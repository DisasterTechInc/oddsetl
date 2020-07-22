import os
import shutil


def cleanup_the_house(dir_lst):
    """Given a list of directories, flush dir and subdirs."""

    for dir_to_remove in dir_lst:
        if os.path.exists(dir_to_remove):
            shutil.rmtree(dir_to_remove)

    return


def make_dirs(dir_lst):
    """Given a list of directories, make empty dirs."""

    for dir_to_make in dir_lst:
        if not os.path.exists(dir_to_make):
            os.mkdir(dir_to_make)

    return
