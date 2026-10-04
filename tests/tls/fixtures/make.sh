#!/bin/sh
# Makes the certificates the TLS worlds use (tls.md, 5), with OpenSSL 3.4 or
# later. They are kept as DER, and made again only when one must change.
#
# - root.der: the root the client trusts, "skein test root".
# - intermediate.der: an intermediate it signed.
# - leaf.der, leaf.key: the server's certificate, signed by the intermediate,
#   for skein.test and 127.0.0.1, and its key (PKCS #8).
# - big.der: the same key, for big.skein.test and 1,500 more names, about
#   40 KB, so that the message carrying it spans several records.
# - other.der: the same key, for skein.test, signed by a root no one trusts.
#
# Every certificate is valid from 2026-01-01 to 2036-01-01: a world checks
# them at the wall time it chooses (env.wall), never the clock.
set -eu

cd "$(dirname "$0")"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

from=20260101000000Z
until=20360101000000Z

key() {
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$scratch/$1.pem"
}

key root
openssl req -x509 -new -key "$scratch/root.pem" -subj "/CN=skein test root" \
    -not_before $from -not_after $until \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$scratch/root.crt"

key intermediate
openssl req -x509 -new -key "$scratch/intermediate.pem" -subj "/CN=skein test intermediate" \
    -CA "$scratch/root.crt" -CAkey "$scratch/root.pem" \
    -not_before $from -not_after $until \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$scratch/intermediate.crt"

key leaf
openssl req -x509 -new -key "$scratch/leaf.pem" -subj "/CN=skein.test" \
    -CA "$scratch/intermediate.crt" -CAkey "$scratch/intermediate.pem" \
    -not_before $from -not_after $until \
    -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=serverAuth" -addext "subjectAltName=DNS:skein.test,IP:127.0.0.1" \
    -out "$scratch/leaf.crt"

names="DNS:big.skein.test"
i=0
while [ $i -lt 1500 ]; do
    names="$names,DNS:name-$i.big.skein.test"
    i=$((i + 1))
done
openssl req -x509 -new -key "$scratch/leaf.pem" -subj "/CN=big.skein.test" \
    -CA "$scratch/intermediate.crt" -CAkey "$scratch/intermediate.pem" \
    -not_before $from -not_after $until \
    -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=serverAuth" -addext "subjectAltName=$names" \
    -out "$scratch/big.crt"

key other
openssl req -x509 -new -key "$scratch/other.pem" -subj "/CN=skein untrusted root" \
    -not_before $from -not_after $until \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$scratch/other-root.crt"
openssl req -x509 -new -key "$scratch/leaf.pem" -subj "/CN=skein.test" \
    -CA "$scratch/other-root.crt" -CAkey "$scratch/other.pem" \
    -not_before $from -not_after $until \
    -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=serverAuth" -addext "subjectAltName=DNS:skein.test,IP:127.0.0.1" \
    -out "$scratch/other.crt"

for name in root intermediate leaf big other; do
    openssl x509 -in "$scratch/$name.crt" -outform DER -out "$name.der"
done
openssl pkcs8 -topk8 -nocrypt -in "$scratch/leaf.pem" -outform DER -out leaf.key
