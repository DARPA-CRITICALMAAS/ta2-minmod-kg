# stop on error
set -e

INFILE=$1
DBLOC=$2

# read list of files into array
readarray -t files < $INFILE

batch_size=10

for((i=0; i < ${#files[@]}; i+=batch_size))
do
  part=( "${files[@]:i:batch_size}" )
#   tdb2.tdbloader --loc=$DBLOC "${part[@]}"
    echo "${part[@]}"
done