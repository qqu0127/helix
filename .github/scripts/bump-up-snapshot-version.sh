current_version=`grep -A 1 "<artifactId>helix</artifactId>" pom.xml | grep "<version>" | awk 'BEGIN {FS="[<,>]"};{print $3}'`
if [ "$#" -eq 1 ]; then
  new_version=$1
else
  timestamp=`date +'%Y%m%d'`
  new_version=`echo $current_version | cut -d'-' -f1`-dev-$timestamp
fi
./bump-up.command $current_version $new_version