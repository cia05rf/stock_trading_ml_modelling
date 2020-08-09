import os

def replace_file(_old_file:str,_new_file:str):
    """Function to delete an old file nad then replace it with aother one
    
    args:
    ------
    _old_file - str - filepath for the file to be removed
    _new_file - str - filepath for the fiel to replace the old file

    returns:
    ------
    True if successful, exception thrown if not 
    """
    try:
        os.remove(_old_file)
        print('\nSUCCESSFULLY REMOVED {}'.format(_old_file))
    except Exception as e:
        print('\WARNING - COULD NOT REMOVE {}:{}'.format(_old_file,e))
        raise Warning(e)
    try:
        os.rename(_new_file,_old_file)
        print('\nSUCCESSFULLY RENAMED {} TO {}'.format(_new_file,_old_file))
    except Exception as e:
        print('\nERROR - RENAMING:{}'.format(e))
        raise Exception(e)
    return True