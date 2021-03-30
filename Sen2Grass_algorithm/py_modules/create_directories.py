# import modules
import os

# define function(s)
def create_directories():
    """
    Function to create local directories to store data and python modules.
    """
    folders = ["data", "py_modules"]
    for i in range(len(folders)):
        path = f"./{folders[i]}"
        if not os.path.isdir(path):
            os.mkdir(path)