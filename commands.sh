#!/bin/sh

export $(grep -v '^#' .env | xargs -d '\n')

# target = $ARGUMENTS['target']
# echo $1
# dbt build --profiles-dir $(pwd) --target target
# dbt test --select source:* --profiles-dir $(pwd)
# dbt docs generate --profiles-dir $(pwd)
# dbt docs serve --port 8083 --profiles-dir $(pwd)

while [ $# -gt 0 ]; do
  case "$1" in
    --target)
      target="$2"
      ;;
    --command)
      command="$2"
      ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
  shift
done

echo "Running dbt $command --target $target --profiles-dir $(pwd)"

dbt $command --target $target --profiles-dir $(pwd)