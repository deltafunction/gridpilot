#!/bin/sh

printf "$1 : "
jarsigner -keystore suresh.store -verify -certs $1