#!/bin/bash

#rm out.log
#curl -i -H "X-Auth-User:SLOS530867-3:SL530867" -H "X-Auth-Key:46a4ba15b984f2bda6ec9132a9b6205d5c618bdb0cc18e74c121ad0e9f2ca059" https://dal05.objectstorage.softlayer.net/auth/v1.0 | tee out.log
#token=$(cat out.log | grep "^X-Auth-Token:" | grep -Eo "AUTH_.*")
#url=$(cat out.log | grep "^X-Storage-Url:" | grep -Eo "https.*" | sed 's/[\r\n]*$//') 

token=$1
url="https://dal05.objectstorage.softlayer.net/v1/AUTH_0529dd31-c208-45b4-b11e-584820a89a92"
file="ngram-1GB.csv"
#file="a.txt"
echo -e "[INFO]Token: $token"
echo "[INFO]URL: $url"

container="container1"

echo "[INFO] Deleting Container"
curl -i -XDELETE -H X-Auth-Token:${token} ${url}/${container}

echo "[INFO] Listing Containers"
curl -s -i -H "X-Auth-Token:$token" $url

echo -e "[INFO] Creating Container: ${container}\n"
curl -i -XPUT -H X-Auth-Token:${token} ${url}/${container}

echo "[INFO] Listing Containers"
curl -s -i -H "X-Auth-Token:$token" $url

echo "[INFO] Uploading/Downloading Data Sequentially.."
for i in {1..2}
do
	echo "[INFO] Uploading/Writing Data - $i"
	start=$(date +"%s")
	curl -i -XPUT -H X-Auth-Token:${token} ${url}/${container}/${file}-$i -d @${file}
	end=$(date +"%s")
	diff=$(( $end - $start ))
	echo "[INFO] $(($diff / 60)) minutes and $(($diff % 60)) seconds elapsed."
	echo "[INFO] Transfer Rate: $((1024*1024 / $diff)) kb/sec"

	echo "[INFO] Listing Container Contents"
	curl -s -i -H "X-Auth-Token:$token" $url/${container}

	echo "[INFO] Downloading/Reading Data - $i"
	start=$(date +"%s")
	curl -i -H X-Auth-Token:${token} ${url}/${container}/${file}-$i -o ${file}-$i 
	end=$(date +"%s")
        diff=$(( $end - $start ))
        echo "[INFO] $(($diff / 60)) minutes and $(($diff % 60)) seconds elapsed."
        echo "[INFO] Transfer Rate: $((1024*1024 / $diff)) kb/sec"
done

echo "[INFO] Uploading/Downloading Data in Parallel.."
start=$(date +"%s")
cmd="curl -i -XPUT -H X-Auth-Token:${token} ${url}/${container}/${file}-p -d @${file}"
echo $cmd
eval $cmd &
pid1=$!

cmd="curl -i -H X-Auth-Token:${token} ${url}/${container}/${file}-$i -o ${file}-p"
echo $cmd
eval $cmd &
pid2=$!

wait $pid1
wait $pid2

end=$(date +"%s")
diff=$(( $end - $start ))
echo "[INFO] $(($diff / 60)) minutes and $(($diff % 60)) seconds elapsed."
echo "[INFO] Transfer Rate: $((1024*1024 / $diff)) kb/sec"



