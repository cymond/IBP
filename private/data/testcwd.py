import os

print("os.get_cwd(): ", os.getcwd())

dir_filename = "../data/admin/directories.csv"
print("os.get_cwd(): ", os.getcwd())
if not os.path.isfile(dir_filename):
    print("File not accessible: ", dir_filename)
else:
    print("File accessible", dir_filename)

print("hello")
dir_filename = "../data/admin/directories.csv"
print("os.get_cwd(): ", os.getcwd())
if not os.path.isfile(dir_filename):
    # logger.error("The file, {}, does not exist".format(dir_filename))
    print("File not accessible: ", dir_filename)
else:
    print("File accessible: ", dir_filename)