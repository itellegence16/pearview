import os
dir_name = os.getcwd()
parent_dir = os.path.abspath(os.path.join(dir_name, os.pardir))
output_dir = os.path.join(parent_dir, "output")
source_dir = os.path.join(parent_dir, "source_input")
