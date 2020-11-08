import pandas as pd
from glob import glob

files = glob("pca_info.csv/*.csv")

# for file in files:
#     print(f"Processing {file}")
#     with open(file, "r") as raw_csv:
#         with open("cleaned2.csv", "a+") as cleaned_csv:
#             lines = raw_csv.readlines()
#             for line in lines:
#                 new_line = line.replace("\"[", "").replace("]\"", "")
#                 cleaned_csv.write(new_line)

df = pd.read_csv('cleaned2.csv', header=None)
plt = df.plot(kind='scatter', x=0, y=1, c=2, colormap='gist_rainbow', colorbar=True, legend=True)
# plt.legend()
figure = plt.get_figure()
figure.savefig('final5.png')
