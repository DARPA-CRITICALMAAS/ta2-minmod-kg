import configparser
from pathlib import Path

import psutil

# get the total memory in bytes
total_memory = psutil.virtual_memory().total
total_memory_in_gb = total_memory / (1024**3)
print("Total memory:", total_memory_in_gb, "GB")

# parse the configuration
config_template = Path(__file__).parent / "virtuoso.ini.template"

config = configparser.ConfigParser()
config.optionxform = str  # type: ignore - make the keys case-sensitive
config.read(config_template)

# change the following values based on memory available
# we use 66% of free system memory as recommended by Virtuoso
DB_PERCENTAGE = 0.66

config["Parameters"]["NumberOfBuffers"] = str(
    int(total_memory * DB_PERCENTAGE / (8 * 1024))
)
config["Parameters"]["MaxDirtyBuffers"] = str(
    int(int(config["Parameters"]["NumberOfBuffers"]) * 3 / 4)
)

print(
    f"Use {DB_PERCENTAGE}% of the free system memory for NumberOfBuffers and MaxDirtyBuffers:"
)
for key in ["NumberOfBuffers", "MaxDirtyBuffers"]:
    print("\t-", key, config["Parameters"][key])

# now the rest of the memory can be used for query processing
# ideally, we want to support at least two users - max 10 users
# if we deploy on server without enough memory, we can't satisfy all
# requirements.
if (1 - DB_PERCENTAGE) * total_memory_in_gb * 2 >= 4:
    config["Parameters"]["MaxQueryMem"] = "4G"
    # config["Parameters"]["MaxClientConnections"] = str(
    #     min(max(2, int((1 - DB_PERCENTAGE) * total_memory_in_gb / 4)), 10)
    # )
else:
    config["Parameters"]["MaxQueryMem"] = "2G"
    # config["Parameters"]["MaxClientConnections"] = str(
    #     min(max(2, int((1 - DB_PERCENTAGE) * total_memory_in_gb / 2)), 10)
    # )

# config["Parameters"]["MaxClientConnections"] = "10"
# config["HTTPServer"]["MaxClientConnections"] = config["Parameters"][
#     "MaxClientConnections"
# ]
print("MaxQueryMem:", config["Parameters"]["MaxQueryMem"])
print("MaxClientConnections:", config["Parameters"]["MaxClientConnections"])

with open(Path(__file__).parent / "virtuoso.ini", "w") as configfile:
    config.write(configfile)
