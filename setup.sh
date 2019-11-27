#!/usr/bin/env bash
# following https://coffeateam.github.io/coffea/installation.html#creating-a-portable-virtual-environment

NAME=coffeaenv
LCG=/cvmfs/sft.cern.ch/lcg/views/LCG_96python3/x86_64-centos7-gcc8-opt/setup.sh

source $LCG
python -m venv --copies $NAME
source $NAME/bin/activate
python -m pip install setuptools pip --upgrade
# here we install the ttgamma package and its dependencies
python -m pip install -e .

sed -i '40s/.*/VIRTUAL_ENV="$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}" )")" \&\& pwd)"/' $NAME/bin/activate
sed -i '1s/#!.*python$/#!\/usr\/bin\/env python/' $NAME/bin/*
sed -i "2a source ${LCG}" $NAME/bin/activate
tar -zcf ${NAME}.tar.gz ${NAME}
