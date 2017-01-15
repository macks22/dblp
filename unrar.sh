#!/bin/bash

if [[ ! -f rar/unrar ]]; then
    if [ "$(uname)" == "Darwin" ]; then
        wget http://www.rarlab.com/rar/rarosx-5.4.0.tar.gz
        tar -xzvf rarosx-*.tar.gz
        rm rarosx-*.tar.gz
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        wget http://www.rarlab.com/rar/rarlinux-5.3.b4.tar.gz
        tar -zxvf rarlinux-*.tar.gz
        rm rarlinux-*.tar.gz
    elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ]; then
        echo "Download WinRAR for Windows extraction at:"
        echo "http://www.rarlab.com/rar/winrar-x64-540.exe"
        exit
    fi
fi

./rar/unrar e -o+ $1
fname=$(basename $1)
base=${fname%.*}
mv $base.txt $2
