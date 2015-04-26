#!/bin/sh

#flush MySQL
$(mysqladmin -h$1 -u$2 -p$3 flush-hosts)
