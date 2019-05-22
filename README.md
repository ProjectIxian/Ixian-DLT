# IXIAN Project

## About IXIAN

Ixian DLT is a revolutionary blockchain that brings several innovative advantages, such as processing a high volume of micro-transactions quickly while consuming a low amount of processing power, disk space and energy. 

**Homepage**: [IXIAN Project Homepage](https://www.ixian.io "IXIAN")

**Discord**: [Ixian Community](https://discord.gg/P493UN9)

**Bitcointalk**: [ANN Thread](https://bitcointalk.org/index.php?topic=4631942.0)

## The repository

The IXIAN GitHub project is divided into seven main parts:

*[Ixian-Core](https://github.com/ProjectIxian/Ixian-Core): Functionality common to all other projects.
*[Ixian-DLT](https://github.com/ProjectIxian/Ixian-DLT): Implementation of the blockchain-processing part (the Master Node software).
*[Ixian-S2](https://github.com/ProjectIxian/Ixian-S2): Implementation of the streaming network (the S2 Node software).
*[SPIXI](https://github.com/ProjectIxian/SPIXI): Implementation of the SPIXI messaging client for Windows, Android and iOS.
*[Ixian-Miner](https://github.com/ProjectIxian/Ixian-Miner): Implementation of the Ixian standalone mining software.
*[Ixian-LiteWallet](https://github.com/ProjectIxian/Ixian-LiteWallet): Simple CLI wallet for the Ixian DLT network.
*[Ixian-Pool](https://github.com/ProjectIxian/Ixian-Pool): Mining pool software.

## Development branches

There are two main development branches:
* master: This branch is used to build the binaries for the official IXIAN DLT network. It should change slowly and be quite well-tested. This is also the default branch for anyone who wishes to build their Ixian software from source.
* development: This is the main development branch and the source for testnet binaries. The branch might not always be kept bug-free, if an extensive new feature is being worked on. If you are simply looking to build a current testnet binary yourself, please use one of the release tags which will be associated with the master branch.

## Running
Download the latest binary release or you can compile the code yourself.
### Windows
Double-click the corresponding .bat file in the IxianDLT directory to quickly start.

or

Open a terminal in the IxianDLT directory and type
```
IxianDLT.exe -h
```
to find out how to configure and run the IxianDLT node.

### Linux
Download and install the latest Mono release for your Linux distribution. The default Mono versions shipped with most common distributions are outdated.

Go to the [Mono official website](https://www.mono-project.com/download/stable/#download-lin) and follow the steps for your Linux distribution.
We recommend you install the **mono-complete** package.

Open a terminal and navigate to the IxianDLT folder, then type
```
mono IxianDLT.exe -h
```
to find out how to configure and run the IxianDLT node.

## Building
### Windows
Visual Studio 2017 is required (Community Edition is fine), you can get it from here: [Visual Studio](https://visualstudio.microsoft.com/)

Several NuGetPackages are downloaded automatically during the build process.

### Linux
Download and install the latest Mono release for your Linux distribution. The default Mono versions shipped with most common distributions are outdated.

Go to the [Mono official website](https://www.mono-project.com/download/stable/#download-lin) and follow the steps for your Linux distribution.

We recommend you install the **mono-complete** package.

For Debian based distributions such as Ubuntu, type
```
sudo apt install mono-complete nuget msbuild git gcc
```
or if you have a Redhat based distribution, type
```
sudo yum install mono-complete nuget msbuild git gcc
```

Next you'll need to build the Ixian solution. You can do this by typing the following commands in the terminal:
```
git clone --recurse-submodules https://github.com/ProjectIxian/Ixian-DLT.git
cd Ixian-DLT
nuget restore DLTNode.sln
msbuild DLTNode.sln /p:Configuration=Release
```
The IxianDLT will be compiled and placed in the IxianDLT/bin/Release/ folder.

## DLT Hybrid PoW Miner

The DLT project uses an argon2 shared library to mine Ixian DLT blocks. Our implementation comes from this project: [Argon2 Reference implementation](https://github.com/P-H-C/phc-winner-argon2)

## Get in touch / Contributing

If you feel like you can contribute to the project, or have questions or comments, you can get in touch with the team through Discord: (https://discord.gg/dbg9WtR)

## Pull requests

If you would like to send an improvement or bugfix to this repository, but without permanently joining the team, follow these approximate steps:

1. Fork this repository
2. Create a branch (preferably with a name that describes the change)
3. Create commits (the commit messages should contain some information on what and why was changed)
4. Create a pull request to this repository for review and inclusion.
