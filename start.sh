#!/bin/sh -e
echo Rebuilding Ixian DLT...
echo Cleaning previous build
dotnet clean --configuration Release
echo Restoring packages
dotnet restore
echo Building DLT
dotnet build --configuration Release
echo Cleaning Argon2
cd Argon2_C
make clean
echo Building Argon2
make
echo Copying Argon2 library to release directory
cp libargon2.1.dylib ../IxianDLT/bin/Release/net6.0/libargon2.dylib
cd ..
echo Done rebuilding Ixian DLT
cd IxianDLT/bin/Release/net6.0/
dotnet IxianDLT.dll --disableWebStart --walletPassword '(*&^Bubba123)' 
