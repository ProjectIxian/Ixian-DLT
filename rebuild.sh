#!/bin/sh -e
echo Rebuilding Ixian DLT...
echo Cleaning previous build
msbuild DLTNode.sln /p:Configuration=Release /target:DLTNode:Clean
echo Removing packages
rm -rf packages
echo Restoring packages
nuget restore DLTNode.sln
echo Building DLT
msbuild DLTNode.sln /p:Configuration=Release /target:DLTNode
echo Cleaning Argon2
cd Argon2_C
make clean
echo Building Argon2
make
echo Copying Argon2 library to release directory
cp libargon2.so.1 ../IxianDLT/bin/Release/libargon2.so
cd ..
echo Done rebuilding Ixian DLT