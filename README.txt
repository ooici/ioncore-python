This is the project for the OOI Release 1 LCA architecture prototype.

It contains the core services and their architectural dependencies, and relies on selected
external packages, such as Magnet, etc.

Get it with

>git clone git://amoeba.ucsd.edu/lcaarch.git

Start CC ("Magnet" Python Capability Container) shell in directory with:

>twistd -n magnet -h amoeba.ucsd.edu shell

Start system by executing within the CC shell:

><> from ion.core import bootstrap
><> bootstrap.start()

