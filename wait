#!/bin/sh

(sleep $1; kill $$ 2>/dev/null) &
until nc -z 127.0.0.1 9042
do
    sleep 1;
done
